from Thread_Manager import thread_manager, state, request, priority
from Buffer_Manager import buffer_manager


class catalog_manager(buffer_manager):

    catalog_buffer = {}
    user_buffer = {}
    privilege_buffer = {}

    def __init__(self):
        catalog_manager.catalog_buffer = self.read_index('db_files/catalog.json')
        catalog_manager.user_buffer = self.read_index('db_files/user.json')
        catalog_manager.privilege_buffer = self.read_index('db_files/privilege.json')

    def read_catalog(self):
        """
        返回catalog_buffer
        """
        # 放入队列
        req = request('db_files/catalog.json', priority.read)
        thread_manager.lock.acquire()
        thread_manager.wait_list.append(req)
        thread_manager.lock.release()
        while req.state != state.doing:
            pass
        catalog_buffer = catalog_manager.catalog_buffer
        req.state = state.done
        return catalog_buffer

    def write_catalog(self, catalog_buffer):
        """
        更新catalog_buffer
        """
        # 放入队列
        req = request('db_files/catalog.json', priority.write)
        thread_manager.lock.acquire()
        thread_manager.wait_list.append(req)
        thread_manager.lock.release()
        while req.state != state.doing:
            pass
        catalog_manager.catalog_buffer = catalog_buffer
        req.state = state.done

    def read_user(self):
        """
        返回user_buffer
        """
        # 放入队列
        req = request('db_files/user.json', priority.read)
        thread_manager.lock.acquire()
        thread_manager.wait_list.append(req)
        thread_manager.lock.release()
        while req.state != state.doing:
            pass
        user_buffer = catalog_manager.user_buffer
        req.state = state.done
        return user_buffer

    def write_user(self, user_buffer):
        """
        更新catalog_buffer
        """
        # 放入队列
        req = request('db_files/user.json', priority.write)
        thread_manager.lock.acquire()
        thread_manager.wait_list.append(req)
        thread_manager.lock.release()
        while req.state != state.doing:
            pass
        catalog_manager.user_buffer = user_buffer
        req.state = state.done

    def read_privilege(self):
        """
        返回privilege_buffer
        """
        # 放入队列
        req = request('db_files/privilege.json', priority.read)
        thread_manager.lock.acquire()
        thread_manager.wait_list.append(req)
        thread_manager.lock.release()
        while req.state != state.doing:
            pass
        privilege_buffer = catalog_manager.privilege_buffer
        req.state = state.done
        return privilege_buffer

    def write_privilege(self, privilege_buffer):
        """
        更新catalog_buffer
        """
        # 放入队列
        req = request('db_files/privilege.json', priority.write)
        thread_manager.lock.acquire()
        thread_manager.wait_list.append(req)
        thread_manager.lock.release()
        while req.state != state.doing:
            pass
        catalog_manager.privilege_buffer = privilege_buffer
        req.state = state.done

    def __del__(self):  # 必须在子进程里面写回，因为两个主进程是放在一起进行的
        self.write_index('db_files/catalog.json', catalog_manager.catalog_buffer)
        self.write_index('db_files/user.json', catalog_manager.user_buffer)
        self.write_index('db_files/privilege.json', catalog_manager.privilege_buffer)
