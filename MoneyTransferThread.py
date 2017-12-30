import threading
import time
import sys
import bank_pb2
from random import randint
from BankVault import BankVault
from ThreadPool import ThreadPool

MIN_SLEEP_TIME = 1
MAX_SLEEP_TIME = 5


# Periodically sleeps for a random amount of time. After waking up, sends a random amount of money to a randomly
# selected branch (if enabled) and sleeps again.
class MoneyTransferThread(threading.Thread):

    enabled = True
    enabled_lock = threading.Lock()

    def __init__(self, initial_balance):
        threading.Thread.__init__(self)
        self.initial_balance = initial_balance

    @classmethod
    def set_enabled(cls, new_value):
        cls.enabled_lock.acquire()
        cls.enabled = new_value
        cls.enabled_lock.release()

    @classmethod
    def get_enabled(cls):
        cls.enabled_lock.acquire()
        val = cls.enabled
        cls.enabled_lock.release()
        return val

    def run(self):
        # Record the initial balance
        BankVault.set_balance(self.initial_balance)
        print "Branch initialized with initial balance : " + str(self.initial_balance)

        # Mandatory initial sleep, to make sure all branches process the init_branch message.
        time.sleep(MAX_SLEEP_TIME)

        while True:
            sleep_period = randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME)
            time.sleep(sleep_period)

            if MoneyTransferThread.get_enabled():
                transfer_msg = bank_pb2.Transfer()
                pb_msg = bank_pb2.BranchMessage()

                # Send a random amount of money to a random branch
                new_friend = ThreadPool.get_random_thread()
                money_to_send = BankVault.reduce_by_random_percentage()

                if money_to_send == 0:
                    # Branch has ran out of money
                    sys.exit(1)

                transfer_msg.money = money_to_send

                pb_msg.transfer.CopyFrom(transfer_msg)

                # Send the money to selected branch
                print "Sending " + str(pb_msg.transfer.money) + " to " + str(new_friend.remote_branch_name) +\
                      ". New balance is : " + str(BankVault.get_balance())
                new_friend.send_msg_to_remote(pb_msg)
