from random import randint


class ThreadPool:
    thread_list = []
    thread_count = 0

    def __init__(self):
        pass

    @classmethod
    def add_thread(cls, new_thread):
        # TODO: Does this need to be thread-safe? :D
        cls.thread_list.append(new_thread)
        cls.thread_count += 1

    @classmethod
    def get_thread(cls, thread_no):
        try:
            return cls.thread_list[thread_no]
        except IndexError:
            print "Error! The thread with given index " + str(thread_no) + " doesn't exist."

    @classmethod
    def get_thread_count(cls):
        return cls.thread_count

    @classmethod
    def get_random_thread(cls):
        if cls.thread_count == 0:
            return None
        return cls.thread_list[randint(0, cls.thread_count-1)]
