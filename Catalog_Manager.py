from Buffer_Manager import buffer_manager

class catalog_manager(buffer_manager):
    '''
    记录表的数据
    '''
    catalog_buffer = {}

    def __init__(self):
        buffer_manager.catalog_buffer = self.read_index('db_files/catalog.json')

