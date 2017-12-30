#!/usr/bin/python

import sys
import socket
import threading
sys.path.append('/home/phao3/protobuf/protobuf-3.4.0/python')
import bank_pb2

from BankVault import BankVault
from ThreadPool import ThreadPool
from MoneyTransferThread import MoneyTransferThread

MAX_SIMULTANEOUS_CONNECTIONS = 100
MAX_REQUEST_SIZE = 10000

init_received = False
branch_name = None


# Handles all snapshot related messages and maintains local snapshot information
class SnapshotHandler:
    current_snapshots = {}
    marker_handler_lock = threading.Lock()

    def __init__(self):
        pass

    @classmethod
    def handle_init_snapshot(cls, incoming_message):

        # Temporarily pause the money transfer
        MoneyTransferThread.set_enabled(False)

        # Grab the lock, since the current_snapshots data structure we modify ahead is shared between threads
        cls.marker_handler_lock.acquire()

        # Make sure that the new snapshot_id doesn't already exist
        snapshot_id = incoming_message.init_snapshot.snapshot_id
        if snapshot_id in cls.current_snapshots:
            print "ERROR! Got init snapshot again for snapshot id : " + str(snapshot_id)
            return

        print "Got init_snapshot msg (snapshot_id: " + str(snapshot_id) + ")"

        # Record local state
        current_balance = BankVault.get_balance()
        state = {"local": current_balance}
        cls.current_snapshots[snapshot_id] = state

        # Create marker message
        marker_msg = bank_pb2.Marker()
        marker_msg.snapshot_id = snapshot_id

        pb_msg = bank_pb2.BranchMessage()
        pb_msg.marker.CopyFrom(marker_msg)

        # Send marker message to everyone else
        total_peers = ThreadPool.get_thread_count()
        for i in range(total_peers):
            a_friend = ThreadPool.get_thread(i)

            if not a_friend:
                continue

            print "Sending marker message to : " + str(a_friend.remote_branch_name) + " (snapshot_id: "\
                  + str(snapshot_id) + ")"
            a_friend.send_msg_to_remote(pb_msg)

            # Start recording all incoming activity
            a_friend.add_recorder(snapshot_id)

        # Release the lock, we're done modifying the shared data structure
        cls.marker_handler_lock.release()

        # Resume the money transfer thread
        MoneyTransferThread.set_enabled(True)

    @classmethod
    def handle_marker(cls, incoming_message, remote_branch_name):

        # Temporarily pause the money transfer
        MoneyTransferThread.set_enabled(False)

        # Grab the lock, since the current_snapshots data structure we modify ahead is shared between threads
        cls.marker_handler_lock.acquire()

        snapshot_id = incoming_message.marker.snapshot_id
        if snapshot_id in cls.current_snapshots:
            # This is a reply marker message
            print "Got reply marker msg from: " + str(remote_branch_name) + " (snapshot_id: " + str(snapshot_id) + ")"

            # Get the state of the channel on which this marker was received
            total_peers = ThreadPool.get_thread_count()
            for i in range(total_peers):

                a_friend = ThreadPool.get_thread(i)
                if not a_friend:
                    continue

                if a_friend.remote_branch_name == remote_branch_name:
                    # Channel found, get the money recorded on this channel
                    money_in_flight = a_friend.pop_recorder(snapshot_id)

                    # Record the state of the channel
                    cls.current_snapshots[snapshot_id][str(remote_branch_name)] = money_in_flight
                    break

        else:
            # This is the first marker message we're seeing
            print "Got the first marker msg from " + str(remote_branch_name) + " (snapshot_id: "\
                  + str(snapshot_id) + ")"

            # Record local state
            current_balance = BankVault.get_balance()
            state = {"local": current_balance}
            cls.current_snapshots[snapshot_id] = state

            # Record the state of the incoming channel from the sender to itself as empty
            cls.current_snapshots[snapshot_id][str(remote_branch_name)] = 0

            # Create marker message
            marker_msg = bank_pb2.Marker()
            marker_msg.snapshot_id = snapshot_id

            pb_msg = bank_pb2.BranchMessage()
            pb_msg.marker.CopyFrom(marker_msg)

            # Send marker msg to all outgoing channels, except self
            total_peers = ThreadPool.get_thread_count()
            for i in range(total_peers):
                a_friend = ThreadPool.get_thread(i)
                if not a_friend:
                    continue

                '''# MY HACK BEGIN --- ONLY FOR TESTING
                if a_friend.remote_branch_name == remote_branch_name:
                    transfer_msg_h = bank_pb2.Transfer()
                    pb_msg_h = bank_pb2.BranchMessage()

                    transfer_msg_h.money = 10

                    pb_msg_h.transfer.CopyFrom(transfer_msg_h)

                    a_friend.send_msg_to_remote(pb_msg_h)
                # MY HACK END ---'''

                print "Sending marker message to : " + str(a_friend.remote_branch_name) + " (snapshot_id: " \
                      + str(snapshot_id) + ")"
                a_friend.send_msg_to_remote(pb_msg)

                # Start recording all incoming activity
                if a_friend.remote_branch_name != remote_branch_name:
                    a_friend.add_recorder(snapshot_id)

        # Release the lock, we're done modifying the shared data structure
        cls.marker_handler_lock.release()

        # Resume the money transfer thread
        MoneyTransferThread.set_enabled(True)

    @classmethod
    def handle_retrieve_snapshot(cls, incoming_message):
        snapshot_id = incoming_message.retrieve_snapshot.snapshot_id

        if snapshot_id not in cls.current_snapshots:
            # Asked to retrieve a snapshot which doesn't exist
            print "ERROR! Asked to retrieve a snapshot ID (" + str(snapshot_id) + ") which doesn't exist!"
            return None

        # Create the return_snapshot object and local_snapshot object
        pb_msg = bank_pb2.BranchMessage()
        return_snapshot_obj = bank_pb2.ReturnSnapshot()
        local_snapshot_obj = return_snapshot_obj.LocalSnapshot()

        local_snapshot_obj.snapshot_id = snapshot_id

        # Populate the local_snapshot object
        for state in cls.current_snapshots[snapshot_id]:
            if state != "local":
                local_snapshot_obj.channel_state.append(cls.current_snapshots[snapshot_id][state])
            else:
                local_snapshot_obj.balance = cls.current_snapshots[snapshot_id][state]

        return_snapshot_obj.local_snapshot.CopyFrom(local_snapshot_obj)
        pb_msg.return_snapshot.CopyFrom(return_snapshot_obj)

        return pb_msg


# Handles the init_branch message from controller, establishes connection
def connect_to_branches(init_message):

    if len(init_message.all_branches) == 0:
        # I don't have to connect to any branches!
        print "No branches to connect to!"
        return

    for remote_branch in init_message.all_branches:
        # Connect to a branch only if its name is lexicographically smaller than ours. If greater, the other branch will
        # establish a connection with us.
        if branch_name < remote_branch.name:

            remote_address = (remote_branch.ip, remote_branch.port)
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(remote_address)

            # Create a thread to handle communication with this branch.
            client_thread = ClientThread(client_socket, remote_address, remote_branch.name)
            client_thread.daemon = True
            client_thread.start()


# An object of class ClientThread is created and thread is started to serve each new connected branch or controller.
class ClientThread(threading.Thread):

    def __init__(self, client_socket, client_address, remote_branch_name=None):
        threading.Thread.__init__(self)
        self.client_socket = client_socket
        self.client_address = client_address
        self.remote_branch_name = remote_branch_name
        self.recorders = {}
        self.recorders_lock = threading.Lock()

    def get_remote_address(self):
        return self.client_address

    # Start recording state of the channel
    def add_recorder(self, recorder_id):
        self.recorders_lock.acquire()
        self.recorders[recorder_id] = 0
        self.recorders_lock.release()

    # Add new amount to the state of the channels being recorded
    def update_recorders(self, amount):
        self.recorders_lock.acquire()
        for recorder in self.recorders:
            self.recorders[recorder] += amount
        self.recorders_lock.release()

    # Stop the recording and get the recorded amount
    def pop_recorder(self, recorder_id):
        self.recorders_lock.acquire()

        temp = int(self.recorders[recorder_id])
        del self.recorders[recorder_id]

        self.recorders_lock.release()
        return temp

    def handle_transfer_message(self, incoming_message):
        # Money received from another branch
        amount = incoming_message.transfer.money

        # Add it to the local balance and update the channel state, if any being recorded
        BankVault.add_amount(amount)
        self.update_recorders(amount)
        print "Received " + str(amount) + " from " + str(self.remote_branch_name) + ". New balance is : "\
              + str(BankVault.get_balance())

    # Generic wrapper around the socket send() primitive to send serialized protobuf messages
    def send_msg_to_remote(self, generic_msg):
        if generic_msg:
            self.client_socket.send(generic_msg.SerializeToString())
        else:
            self.client_socket.send()

    # Start receiving on the channel and handle incoming messages
    def start_handling_messages(self):

        pb_msg = bank_pb2.BranchMessage()
        while True:

            incoming_message = self.client_socket.recv(MAX_REQUEST_SIZE)
            pb_msg.ParseFromString(incoming_message)
            if len(incoming_message) == 0:
                # Didn't read anything from socket
                continue

            # Call specific handlers depending on type of the incoming message
            if pb_msg.HasField("transfer"):
                self.handle_transfer_message(pb_msg)

            elif pb_msg.HasField("init_snapshot"):
                SnapshotHandler.handle_init_snapshot(pb_msg)

            elif pb_msg.HasField("marker"):
                SnapshotHandler.handle_marker(pb_msg, self.remote_branch_name)

            elif pb_msg.HasField("retrieve_snapshot"):
                reply = SnapshotHandler.handle_retrieve_snapshot(pb_msg)
                self.send_msg_to_remote(reply)
                if not reply:
                    sys.exit(1)

            else:
                print "ERROR! Message type not identified. This is the message : " + str(pb_msg)

    def run(self):
        global init_received

        if not init_received:
            # First message we'll receive will always be init
            init_received = True

            init_message = bank_pb2.BranchMessage()

            # Accept the init message from controller
            init_message_from_socket = self.client_socket.recv(MAX_REQUEST_SIZE)

            if len(init_message_from_socket) == 0:
                # The controller just opened the connection but didn't send any message
                print("ERROR! The controller didn't send anything as Init Message!")
                self.client_socket.close()
                return 0

            init_message.ParseFromString(init_message_from_socket)

            # Connect to the list of branches got from init_branch message
            connect_to_branches(init_message.init_branch)

            # Start the thread to send money to these branches
            mt_thread = MoneyTransferThread(init_message.init_branch.balance)
            mt_thread.daemon = True
            mt_thread.start()

            # Start handling further incoming messages from controller
            self.start_handling_messages()

        # Else, we're connected to a branch
        # Let's get introduced.
        self.client_socket.send(branch_name)
        remote_branch_name = self.client_socket.recv(MAX_REQUEST_SIZE)

        self.remote_branch_name = remote_branch_name
        print "Connected to : " + str(remote_branch_name)

        # Add this thread object to the pool of threads such that it can be accessed whenever necessary
        ThreadPool.add_thread(self)

        # Start handling further incoming messages from connected branch
        self.start_handling_messages()


# Listen for connections on given port number and spawn a thread to handle each incoming connection
def start_listener_thread(local_port_no):

    # Set up socket which will listen on specified port
    listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_hostname = socket.gethostname()

    try:
        # Bind socket to a particular port and start listening
        listener_socket.bind(('0.0.0.0', int(local_port_no)))
        listener_socket.listen(MAX_SIMULTANEOUS_CONNECTIONS)

    except socket.error:
        print "Cannot start branch on port {0}. Perhaps someone is already using that port " \
                           "or you don't have enough privileges?".format(local_port_no)
        sys.exit(1)

    print str(branch_name) + " listening on " + str(server_hostname) + ":" + str(local_port_no)
    print "Press Ctrl+C to terminate..."

    while True:
        client_socket, client_address = listener_socket.accept()

        # Create a new thread to serve this peer
        client_thread = ClientThread(client_socket, client_address)
        client_thread.daemon = True
        client_thread.start()


if __name__ == '__main__':

    if len(sys.argv) != 3:
        print "2 Arguments required: Branch name and port number."
        sys.exit(1)

    branch_name = sys.argv[1]
    port_no = sys.argv[2]

    start_listener_thread(port_no)
