from threading import Lock
from enum import IntEnum


class State(IntEnum):
    free = 0
    read = 1
    write = 2


class Request(object):
    """
    对于同一个table，不允许多个进程同时访问
    """

    def __init__(self, table_name=None, state=None):
        self.state = state
        self.table_name = table_name

    def __enter__(self):
        # 当出现一个请求时，将其加入等待列表
        if self.table_name is not None:
            while True:
                Thread_Manager.lock.acquire()
                if self.table_name not in Thread_Manager.state_list \
                        or Thread_Manager.state_list[self.table_name] == State.free:
                    Thread_Manager.state_list[self.table_name] = self.state
                    break
                else:
                    Thread_Manager.lock.release()
        else:
            Thread_Manager.lock.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.table_name is not None:
            # 修改状态，释放资源
            Thread_Manager.state_list[self.table_name] = State.free
        Thread_Manager.lock.release()


class Thread_Manager(object):
    """
    进程管理
    """
    state_list = {}  # 被访问的表状态
    lock = Lock()
