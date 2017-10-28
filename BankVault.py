class BankVault:
    current_balance = 0
    vault_lock = threading.Lock()
    withdraw_min_percent = 1
    withdraw_max_percent = 5

    def __init__(self):
        pass

    @classmethod
    def set_balance(cls, new_balance):
        cls.vault_lock.acquire()
        cls.current_balance = new_balance
        cls.vault_lock.release()

    @classmethod
    def add_amount(cls, new_amount):
        cls.vault_lock.acquire()
        cls.current_balance += new_amount
        cls.vault_lock.release()

    @classmethod
    def get_balance(cls):
        return cls.current_balance

    @classmethod
    def reduce_balance(cls, withdrawal_amount):
        cls.vault_lock.acquire()
        if withdrawal_amount > cls.current_balance:
            print "Error! Cannot withdraw more than what bank already has!"
            return 0
        cls.current_balance -= withdrawal_amount
        cls.vault_lock.release()
        return withdrawal_amount

    @classmethod
    def reduce_by_percentage(cls, amount_percentage):
        cls.vault_lock.acquire()
        amount_to_withdraw = int((amount_percentage / float(100)) * cls.current_balance)

        if amount_to_withdraw > cls.current_balance:
            print "Error! Cannot withdraw more than what bank already has!"
            return 0

        cls.current_balance -= amount_to_withdraw
        cls.vault_lock.release()
        return amount_to_withdraw

    @classmethod
    def reduce_by_random_percentage(cls):
        cls.vault_lock.acquire()
        amount_to_withdraw = int((random.randint(cls.withdraw_min_percent, cls.withdraw_max_percent) / float(100))
                                 * cls.current_balance)

        if amount_to_withdraw > cls.current_balance:
            print "Error! Cannot withdraw more than what bank already has!"
            return 0

        cls.current_balance -= amount_to_withdraw
        cls.vault_lock.release()
        return amount_to_withdraw
