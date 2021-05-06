from Buffer_Manager import buffer_manager, main_buffer_manager, state, request, priority
from threading import Lock

class main_catalog_manager(buffer_manager):

    catalog_buffer = {}
    user_buffer = {}
    privilege_buffer = {}

    progress_list = []  # 正在进行中的任务
    wait_list = []  # 正在等待中的任务
    state_list = {}  # 被访问的文件状态
    lock = Lock()

    def __init__(self):
        main_catalog_manager.catalog_buffer = self.read_index('db_files/catalog.json')
        main_catalog_manager.user_buffer = self.read_index('db_files/user.json')
        main_catalog_manager.privilege_buffer = self.read_index('db_files/privilege.json')

        while len(main_catalog_manager.wait_list) != 0 or not main_buffer_manager.is_quit:
            main_catalog_manager.lock.acquire()
            # 将等待队列优先级较低的先进行
            while len(main_catalog_manager.wait_list) != 0:
                req = main_catalog_manager.wait_list[0]
                if req.address not in main_catalog_manager.state_list \
                        or (main_catalog_manager.state_list[req.address] == priority.read
                            and req.priority == priority.read):
                    req = main_catalog_manager.wait_list.pop(0)
                    main_catalog_manager.progress_list.append(req)
                    req.state = state.doing
                    main_catalog_manager.state_list[req.address] = req.priority  # 更新文件列表
                else:
                    main_catalog_manager.state_list = {}  # 清空状态列表
                    break
            main_catalog_manager.lock.release()

            # 将完成的出队列，由于只有主进程访问，不需要加锁
            while len(main_catalog_manager.progress_list) != 0:
                if main_catalog_manager.progress_list[0].state == state.done:
                    main_catalog_manager.progress_list.pop(0)
                pass

class catalog_manager(buffer_manager):
    '''
    记录表的数据,用户数据,权限数据
    '''
    def read_catalog(self):
        """
        返回catalog_buffer
        """
        # 放入队列
        req = request('db_files/catalog.json', priority.read)
        main_catalog_manager.lock.acquire()
        main_catalog_manager.wait_list.append(req)
        main_catalog_manager.lock.release()
        while req.state != state.doing:
            pass
        catalog_buffer = main_catalog_manager.catalog_buffer
        req.state = state.done
        return catalog_buffer

    def write_catalog(self, catalog_buffer):
        """
        更新catalog_buffer
        """
        # 放入队列
        req = request('db_files/catalog.json', priority.write)
        main_catalog_manager.lock.acquire()
        main_catalog_manager.wait_list.append(req)
        main_catalog_manager.lock.release()
        while req.state != state.doing:
            pass
        main_catalog_manager.catalog_buffer = catalog_buffer
        req.state = state.done

    def read_user(self):
        """
        返回user_buffer
        """
        # 放入队列
        req = request('db_files/user.json', priority.read)
        main_catalog_manager.lock.acquire()
        main_catalog_manager.wait_list.append(req)
        main_catalog_manager.lock.release()
        while req.state != state.doing:
            pass
        user_buffer = main_catalog_manager.user_buffer
        req.state = state.done
        return user_buffer

    def write_user(self, user_buffer):
        """
        更新catalog_buffer
        """
        # 放入队列
        req = request('db_files/user.json', priority.write)
        main_catalog_manager.lock.acquire()
        main_catalog_manager.wait_list.append(req)
        main_catalog_manager.lock.release()
        while req.state != state.doing:
            pass
        main_catalog_manager.user_buffer = user_buffer
        req.state = state.done

    def read_privilege(self):
        """
        返回privilege_buffer
        """
        # 放入队列
        req = request('db_files/privilege.json', priority.read)
        main_catalog_manager.lock.acquire()
        main_catalog_manager.wait_list.append(req)
        main_catalog_manager.lock.release()
        while req.state != state.doing:
            pass
        privilege_buffer = main_catalog_manager.privilege_buffer
        req.state = state.done
        return privilege_buffer

    def write_privilege(self, privilege_buffer):
        """
        更新catalog_buffer
        """
        # 放入队列
        req = request('db_files/privilege.json', priority.write)
        main_catalog_manager.lock.acquire()
        main_catalog_manager.wait_list.append(req)
        main_catalog_manager.lock.release()
        while req.state != state.doing:
            pass
        main_catalog_manager.privilege_buffer = privilege_buffer
        req.state = state.done

    def __del__(self):  # 必须在子进程里面写回，因为两个主进程是放在一起进行的
        self.write_index('db_files/catalog.json', main_catalog_manager.catalog_buffer)
        self.write_index('db_files/user.json', main_catalog_manager.user_buffer)
        self.write_index('db_files/privilege.json', main_catalog_manager.privilege_buffer)
