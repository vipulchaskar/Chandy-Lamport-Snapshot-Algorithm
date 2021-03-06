#!/usr/bin/python

import sys
import socket
import time
sys.path.append('/home/phao3/protobuf/protobuf-3.4.0/python')
import bank_pb2

from random import randint

SNAPSHOT_INTERVAL = 10
SNAPSHOT_RETRIEVE_INTERVAL = 5
MAX_BUFFER_SIZE = 1000


def get_branches_from_input_file(input_file):
    branches = []

    try:
        f = open(input_file, "r")

        for line in f:
            a_line = line.strip().split(' ')

            temp_branch_name = a_line[0]
            temp_ip_addr = a_line[1]
            temp_port_no = a_line[2]

            temp_tuple = (temp_branch_name, temp_ip_addr, temp_port_no)
            branches.append(temp_tuple)

        f.close()
    except IOError:
        print "Could not open input file: " + str(input_file)

    return branches


# Periodically, send init_snapshot to a random branch. After some time, retrieve the snapshot from all branches and
# display it on the screen.
def start_snapshotting(branch_sockets):
    no_sockets = len(branch_sockets)

    snapshot_id = 1

    while True:
        # Wait before sending init_snapshot
        time.sleep(SNAPSHOT_INTERVAL)

        # Create init_snapshot message
        pb_msg = bank_pb2.BranchMessage()
        init_snapshot_msg = bank_pb2.InitSnapshot()
        init_snapshot_msg.snapshot_id = snapshot_id
        pb_msg.init_snapshot.CopyFrom(init_snapshot_msg)

        # Select a random branch and send init_snapshot
        victim = branch_sockets[randint(0, no_sockets-1)]
        victim[0].send(pb_msg.SerializeToString())

        print "\nSent snapshot msg " + str(snapshot_id) + " to " + str(victim[1])

        # Wait before retrieving snapshot
        time.sleep(SNAPSHOT_RETRIEVE_INTERVAL)

        # Create retrieve_snapshot message
        pb_msg = bank_pb2.BranchMessage()
        retrieve_snapshot_msg = bank_pb2.RetrieveSnapshot()
        retrieve_snapshot_msg.snapshot_id = snapshot_id
        pb_msg.retrieve_snapshot.CopyFrom(retrieve_snapshot_msg)

        print "\nsnapshot_id: " + str(snapshot_id)
        # Send retrieve_snapshot to all branches and display the returned message
        for branch in branch_sockets:
            branch[0].send(pb_msg.SerializeToString())

            # Get the reply - return_snapshot
            incoming_msg_from_wire = branch[0].recv(MAX_BUFFER_SIZE)
            if len(incoming_msg_from_wire) == 0:
                print "ERROR! the branch " + branch[1] + " doesn't have snapshot: " + str(snapshot_id)
                continue

            pb_msg_ret = bank_pb2.BranchMessage()
            pb_msg_ret.ParseFromString(incoming_msg_from_wire)

            # Error handling
            if not pb_msg_ret.HasField("return_snapshot"):
                print "ERROR! the branch " + branch[1] + " returned some other message : " + str(pb_msg_ret)
                continue

            # Display on the screen in required format
            output_string = str(branch[1]) + ": " + str(pb_msg_ret.return_snapshot.local_snapshot.balance) + ", "
            channel_states = pb_msg_ret.return_snapshot.local_snapshot.channel_state

            branch_names = [x[1] for x in branch_sockets if x[1] != branch[1]]
            for br_name, channel_state in zip(branch_names, channel_states):
                output_string += br_name + "->" + branch[1] + ": " + str(channel_state) + ", "

            print output_string

        snapshot_id += 1


def send_money_to_branches(total_balance, branches):

    balance_per_branch = int(total_balance) / len(branches)
    print "Sending " + str(balance_per_branch) + " per branch to these branches: " + str([x[0] for x in branches])

    # Generate and populate the init_branch message
    init_branch_msg = bank_pb2.InitBranch()
    init_branch_msg.balance = balance_per_branch

    for branch in branches:
        pb_branch = init_branch_msg.all_branches.add()
        pb_branch.name = branch[0]
        pb_branch.ip = branch[1]
        pb_branch.port = int(branch[2])

    pb_msg = bank_pb2.BranchMessage()
    pb_msg.init_branch.CopyFrom(init_branch_msg)

    branch_sockets = []
    # Establish connections to all branches and send them the init_branch message.
    # Also record the socket objects created which will be used later to send snapshot messages.
    for branch in branches:

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((branch[1], int(branch[2])))
        client_socket.send(pb_msg.SerializeToString())
        branch_sockets.append((client_socket, branch[0]))

    return branch_sockets


# Get branch details from input file, send initial amount to branches and start snapshotting
def main(total_balance, input_file):

    branches = get_branches_from_input_file(input_file)

    branch_sockets = send_money_to_branches(total_balance, branches)

    start_snapshotting(branch_sockets)


if __name__ == '__main__':

    if len(sys.argv) != 3:
        print "Two arguments required. Total balance and name of the input file."
        sys.exit(1)

    total_balance = sys.argv[1]
    input_file = sys.argv[2]

    main(total_balance, input_file)
