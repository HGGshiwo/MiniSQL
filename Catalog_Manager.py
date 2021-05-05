from Buffer_Manager import buffer_manager, main_buffer_manager, state, state_flag
from threading import Lock

class main_catalog_manager(buffer_manager):

    catalog_buffer = {}
    user_manager = {}
    privilege_buffer = {}

    is_quit = False
    data_buffer = {}
    queue =  []
    lock = Lock()

    def __init__(self):
        main_catalog_manager.catalog_buffer = self.read_index('db_files/catalog.json')
        main_catalog_manager.user_buffer = self.read_index('db_files/user.json')
        main_catalog_manager.privilege_buffer = self.read_index('db_files/privilege.json')

        while len(main_catalog_manager.queue) != 0 or not main_buffer_manager.is_quit:
            main_catalog_manager.lock.acquire()
            if len(main_catalog_manager.queue) != 0:
                s = main_catalog_manager.queue.pop(0)
                #print(id(s))
                s.state_flag = state_flag.doing
                while s.state_flag != state_flag.done:
                    pass
                main_catalog_manager.lock.release()
            else:
                main_catalog_manager.lock.release()

class catalog_manager(buffer_manager):
    '''
    记录表的数据,用户数据,权限数据
    '''
    def read_catalog(self):
        '''
        返回catalog_buffer
        '''
        #放入队列
        s = state()
        main_catalog_manager.lock.acquire()
        main_catalog_manager.queue.append(s)
        main_catalog_manager.lock.release()
        while(s.state_flag != state_flag.doing):
            pass
        catalog_buffer = main_catalog_manager.catalog_buffer
        s.state_flag = state_flag.done
        return catalog_buffer

    def write_catalog(self, catalog_buffer):
        '''
        更新catalog_buffer
        '''
        # 放入队列
        s = state()
        main_catalog_manager.lock.acquire()
        main_catalog_manager.queue.append(s)
        main_catalog_manager.lock.release()
        while (s.state_flag != state_flag.doing):
            pass
        main_catalog_manager.catalog_buffer = catalog_buffer
        s.state_flag = state_flag.done
    
    def __del__(self):
        self.write_index('db_files/catalog.json', main_catalog_manager.catalog_buffer)
        self.write_index('db_files/user.json', main_catalog_manager.user_manager)
        self.write_index('db_files/privilege.json', main_catalog_manager.privilege_buffer)
