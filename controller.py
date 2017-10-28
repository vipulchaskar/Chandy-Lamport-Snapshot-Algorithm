#!/usr/bin/python

import sys
import socket
sys.path.append('/home/phao3/protobuf/protobuf-3.4.0/python')
import bank_pb2


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


def send_money_to_branches(total_balance, branches):

    balance_per_branch = int(total_balance) / len(branches)

    print "Sending " + str(balance_per_branch) + " per branch to these branches: " + str(branches)

    init_branch_msg = bank_pb2.InitBranch()
    init_branch_msg.balance = balance_per_branch
    # init_branch_msg.all_branches = None
    for branch in branches:
        pb_branch = init_branch_msg.all_branches.add()
        pb_branch.name = branch[0]
        pb_branch.ip = branch[1]
        pb_branch.port = int(branch[2])

    pb_msg = bank_pb2.BranchMessage()
    pb_msg.init_branch.CopyFrom(init_branch_msg)

    print "This is the message i am gonna send everyone! " + str(pb_msg)

    for branch in branches:
        '''# --- My weird hack ---
        del pb_msg.init_branch.all_branches[0]
        # if len(pb_msg.init_branch.all_branches) == 0:
        #    break
        # --- My weird hack ends ---'''

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((branch[1], int(branch[2])))
        client_socket.send(pb_msg.SerializeToString())
        client_socket.close()


def main(total_balance, input_file):

    branches = get_branches_from_input_file(input_file)
    print(branches)

    send_money_to_branches(total_balance, branches)


if __name__ == '__main__':

    if len(sys.argv) != 3:
        print "Two arguments required. Total balance and name of the input file."
        sys.exit(1)

    total_balance = sys.argv[1]
    input_file = sys.argv[2]

    main(total_balance, input_file)
