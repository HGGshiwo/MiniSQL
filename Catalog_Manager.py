from Buffer_Manager import buffer_manager

class catalog_manager(buffer_manager):
    '''
    记录表的数据,用户数据,权限数据
    '''
    catalog_buffer = {}
    user_manager = {}
    privilege_buffer = {}

    def __init__(self):
        catalog_manager.catalog_buffer = self.read_index('db_files/catalog.json')
        catalog_manager.user_buffer = self.read_index('db_files/user.json')
        catalog_manager.privilege_buffer = self.read_index('db_files/privilege.json')

    def read_catalog(self):
        '''
        返回catalog_buffer
        '''
        return catalog_manager.catalog_buffer

    def write_catalog(self, catalog_buffer):
        '''
        更新catalog_buffer
        '''
        catalog_manager.catalog_buffer = catalog_buffer
    
    def __del__(self):
        self.write_index('db_files/catalog.json', catalog_manager.catalog_buffer)
        self.write_index('db_files/user.json', catalog_manager.user_manager)
        self.write_index('db_files/privilege.json', catalog_manager.privilege_buffer)
