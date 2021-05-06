from threading import Lock
from enum import IntEnum


class state(IntEnum):
    todo = 1
    doing = 2
    done = 3


class priority(IntEnum):
    read = 1
    write = 2


class request(object):
    def __init__(self, address, priority):
        self.state = state.todo
        self.address = address
        self.priority = priority


class thread_manager(object):
    """
    进程管理
    """
    progress_list = []  # 正在进行中的任务
    wait_list = []  # 正在等待中的任务
    state_list = {}  # 被访问的文件状态
    lock = Lock()
    is_quit = False

    def __init__(self):
        while len(thread_manager.wait_list) != 0 or not thread_manager.is_quit:
            thread_manager.lock.acquire()
            # 将等待队列优先级较低的先进行
            while len(thread_manager.wait_list) != 0:
                req = thread_manager.wait_list[0]
                if req.address not in thread_manager.state_list \
                        or (thread_manager.state_list[req.address] == priority.read
                            and req.priority == priority.read):
                    req = thread_manager.wait_list.pop(0)
                    thread_manager.progress_list.append(req)
                    req.state = state.doing
                    thread_manager.state_list[req.address] = req.priority  # 更新文件列表
                else:
                    thread_manager.state_list = {}  # 清空状态列表
                    break
            thread_manager.lock.release()

            # 将完成的出队列，由于只有主进程访问，不需要加锁
            while len(thread_manager.progress_list) != 0:
                if thread_manager.progress_list[0].state == state.done:
                    thread_manager.progress_list.pop(0)
                pass

    def quit(self):
        thread_manager.is_quit = True
