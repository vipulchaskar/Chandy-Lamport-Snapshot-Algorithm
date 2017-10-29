#!/usr/bin/python

import sys
import socket
import threading
import time
sys.path.append('/home/phao3/protobuf/protobuf-3.4.0/python')
import bank_pb2

from random import randint
from BankVault import BankVault
from ThreadPool import ThreadPool


MAX_SIMULTANEOUS_CONNECTIONS = 100
MAX_REQUEST_SIZE = 10000
MIN_SLEEP_TIME = 1
MAX_SLEEP_TIME = 5

ERROR_SOCKET_CANNOT_BIND = "Cannot start branch on port {0}. Perhaps someone is already using that port " \
                           "or you don't have enough privileges?"

init_received = False
branch_name = None


class SnapshotHandler:
    current_snapshots = {}

    def __init__(self):
        pass

    @classmethod
    def handle_init_snapshot(cls, incoming_message):
        snapshot_id = incoming_message.init_snapshot.snapshot_id
        if snapshot_id in cls.current_snapshots:
            print "Error! Got init snapshot again for snapshot id : " + str(snapshot_id)
            return

        # Record local state
        current_balance = BankVault.get_balance()
        state = {"local": current_balance}
        cls.current_snapshots[snapshot_id] = state

        # Send marker message to everyone else
        marker_msg = bank_pb2.Marker()
        marker_msg.snapshot_id = snapshot_id

        pb_msg = bank_pb2.BranchMessage()
        pb_msg.marker.CopyFrom(marker_msg)

        total_peers = ThreadPool.get_thread_count()
        for i in range(total_peers):
            a_friend = ThreadPool.get_thread(i)
            print "Sending marker message to :" + str(a_friend.client_address)
            a_friend.send_msg_to_remote(pb_msg)
            # Start recording all incoming activity
            a_friend.add_recorder(snapshot_id)

    @classmethod
    def handle_marker(cls, incoming_message, sender_address):
        snapshot_id = incoming_message.marker.snapshot_id

        if snapshot_id in cls.current_snapshots:
            print "Not the first time I am receiving this marker msg : " + str(snapshot_id)

            # Get the state of the channel

            total_peers = ThreadPool.get_thread_count()
            for i in range(total_peers):

                a_friend = ThreadPool.get_thread(i)
                if a_friend.get_remote_address() == sender_address:
                    money_in_flight = a_friend.pop_recorder(snapshot_id)

                    # Record the state of the channel
                    cls.current_snapshots[snapshot_id][str(sender_address)] = money_in_flight
                    print "Recorded state of the channel for snapshot " + str(snapshot_id) + " as: " +\
                          str(cls.current_snapshots[snapshot_id])
                    break

        else:
            print "Got the marker msg : " + str(snapshot_id) + " for the first time!"

            # Record local state
            current_balance = BankVault.get_balance()
            state = {"local": current_balance}
            cls.current_snapshots[snapshot_id] = state

            # "records the state of the incoming channel from the sender to itself as empty"
            cls.current_snapshots[snapshot_id][str(sender_address)] = 0

            marker_msg = bank_pb2.Marker()
            marker_msg.snapshot_id = snapshot_id

            pb_msg = bank_pb2.BranchMessage()
            pb_msg.marker.CopyFrom(marker_msg)

            # Send marker msg to all outgoing channels, except self
            total_peers = ThreadPool.get_thread_count()
            for i in range(total_peers):
                a_friend = ThreadPool.get_thread(i)
                a_friend.send_msg_to_remote(pb_msg)
                # Start recording all incoming activity
                if a_friend.client_address != sender_address:
                    a_friend.add_recorder(snapshot_id)
        print "The snapshot state for snapshot id " + str(snapshot_id) + " as of now is " \
              + str(cls.current_snapshots[snapshot_id])

    @classmethod
    def handle_retrieve_snapshot(cls, incoming_message):
        pass


def connect_to_branches(init_message):

    if len(init_message.all_branches) == 0:
        # I don't have to connect to any branches!
        print "No branches to connect to!"
        return

    for remote_branch in init_message.all_branches:
        if branch_name < remote_branch.name:

            remote_address = (remote_branch.ip, remote_branch.port)
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(remote_address)

            client_thread = ClientThread(client_socket, remote_address, remote_branch.name)
            client_thread.daemon = True
            client_thread.start()


class MoneyTransferThread(threading.Thread):

    def __init__(self, initial_balance):
        threading.Thread.__init__(self)
        self.initial_balance = initial_balance

    def run(self):
        BankVault.set_balance(self.initial_balance)
        print "Branch initialized with initial balance : " + str(self.initial_balance)

        # Mandatory initial sleep
        time.sleep(MAX_SLEEP_TIME)

        while True:
            sleep_period = randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME)
            time.sleep(sleep_period)

            transfer_msg = bank_pb2.Transfer()
            pb_msg = bank_pb2.BranchMessage()

            new_friend = ThreadPool.get_random_thread()
            money_to_send = BankVault.reduce_by_random_percentage()

            transfer_msg.money = money_to_send

            pb_msg.transfer.CopyFrom(transfer_msg)

            print "Sending " + str(pb_msg.transfer.money) + " to " + str(new_friend.client_address) +\
                  ". New balance is : " + str(BankVault.get_balance())
            new_friend.send_msg_to_remote(pb_msg)


# An object of class ClientThread is created and thread is started
# to serve each new connected peer
class ClientThread(threading.Thread):

    def __init__(self, client_socket, client_address, remote_branch_name=None):
        threading.Thread.__init__(self)
        self.client_socket = client_socket
        self.client_address = client_address
        self.remote_branch_name = remote_branch_name
        self.recorders = {}

    def get_remote_address(self):
        return self.client_address

    def add_recorder(self, recorder_id):
        self.recorders[recorder_id] = 0

    def update_recorders(self, amount):
        for recorder in self.recorders:
            self.recorders[recorder] += amount

    def pop_recorder(self, recorder_id):
        temp = int(self.recorders[recorder_id])
        del self.recorders[recorder_id]
        return temp

    def handle_transfer_message(self, incoming_message):

        amount = incoming_message.transfer.money
        BankVault.add_amount(amount)
        self.update_recorders(amount)
        print "Received " + str(amount) + " from " + str(self.client_address) + ". New balance is : "\
              + str(BankVault.get_balance())

    def send_msg_to_remote(self, generic_msg):
        self.client_socket.send(generic_msg.SerializeToString())

    def start_handling_messages(self):

        pb_msg = bank_pb2.BranchMessage()
        while True:

            incoming_message = self.client_socket.recv(MAX_REQUEST_SIZE)
            pb_msg.ParseFromString(incoming_message)
            if len(incoming_message) == 0:
                continue

            if pb_msg.HasField("transfer"):
                self.handle_transfer_message(pb_msg)
            elif pb_msg.HasField("init_snapshot"):
                SnapshotHandler.handle_init_snapshot(pb_msg)
            elif pb_msg.HasField("marker"):
                SnapshotHandler.handle_marker(pb_msg, self.client_address)
            elif pb_msg.HasField("retrieve_snapshot"):
                SnapshotHandler.handle_retrieve_snapshot(pb_msg)
            else:
                print "Error! Message type not identified. This is the message : " + str(pb_msg)

    def run(self):
        global init_received

        if not init_received:
            # First message we'll receive will always be init
            # TODO: This is NOT thread safe!
            init_received = True

            init_message = bank_pb2.BranchMessage()

            # Accept the request from peer
            init_message_from_socket = self.client_socket.recv(MAX_REQUEST_SIZE)

            if len(init_message_from_socket) == 0:
                # The client just opened the connection but didn't send any request data
                print("The controller didn't send anything as Init Message! :O")
                self.client_socket.close()
                return 0

            init_message.ParseFromString(init_message_from_socket)

            connect_to_branches(init_message.init_branch)

            mt_thread = MoneyTransferThread(init_message.init_branch.balance)
            mt_thread.daemon = True
            mt_thread.start()

            self.start_handling_messages()

        print "Connected to branch : " + str(self.client_address)
        ThreadPool.add_thread(self)

        self.start_handling_messages()


def start_listener_thread(local_port_no):

    # Set up socket which will listen on specified port
    listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_hostname = socket.gethostname()

    try:
        # Bind socket to a particular port and start listening
        listener_socket.bind(('0.0.0.0', int(local_port_no)))
        # The host address is given as 0.0.0.0 to bind to all available IP addresses of machine since we don't know
        # what interface this server should listen on.
        listener_socket.listen(MAX_SIMULTANEOUS_CONNECTIONS)

    except socket.error:
        print ERROR_SOCKET_CANNOT_BIND.format(local_port_no)
        sys.exit(1)

    print str(branch_name) + " listening on " + str(server_hostname) + ":" + str(local_port_no)
    print "Press Ctrl+C to terminate..."

    while True:
        client_socket, client_address = listener_socket.accept()

        # Create a new thread to serve this client
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
