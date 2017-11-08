import threading
import time
import bank_pb2
from random import randint
from BankVault import BankVault
from ThreadPool import ThreadPool

MIN_SLEEP_TIME = 1
MAX_SLEEP_TIME = 5


class MoneyTransferThread(threading.Thread):

    enabled = True

    def __init__(self, initial_balance):
        threading.Thread.__init__(self)
        self.initial_balance = initial_balance

    @classmethod
    def set_enabled(cls, new_value):
        cls.enabled = new_value

    def run(self):
        BankVault.set_balance(self.initial_balance)
        print "Branch initialized with initial balance : " + str(self.initial_balance)

        # Mandatory initial sleep
        time.sleep(MAX_SLEEP_TIME)

        while True:
            if MoneyTransferThread.enabled:
                sleep_period = randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME)
                time.sleep(sleep_period)

                transfer_msg = bank_pb2.Transfer()
                pb_msg = bank_pb2.BranchMessage()

                new_friend = ThreadPool.get_random_thread()
                money_to_send = BankVault.reduce_by_random_percentage()

                transfer_msg.money = money_to_send

                pb_msg.transfer.CopyFrom(transfer_msg)

                print "Sending " + str(pb_msg.transfer.money) + " to " + str(new_friend.remote_branch_name) +\
                      ". New balance is : " + str(BankVault.get_balance())
                new_friend.send_msg_to_remote(pb_msg)
