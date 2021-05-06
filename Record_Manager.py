import os
import struct
from Catalog_Manager import catalog_manager

class record_manager(catalog_manager):
    '''
    主要功能为实现数据文件的创建与删除（由表的定义与删除引起）记录的插入、删除与查找操作
    '''
    def __init__(self):
        self.max_size = 4*1024
    
    def create_data(self, table_name, column_list, fmt, primary_key):
        """
        table_name: 表的名称
        column_list: dict{name, is_unique}
        primary_key: 主键
        """
        if(primary_key == None):
            primary_key = column_list.keys[0]
        
        # 开始更新catalog_buffer[table_name]
        table = {}
        table['name'] = table_name
        table['data_num'] = 1
        table['index_num'] = 0
        table['fmt'] = fmt
        table['column_list'] = column_list
        table['index_list'] = {primary_key: 'db_files/index/' + table_name + '/1.json'}
        table['primary_key'] = primary_key
        address = 'db_files/data/' + table_name
        os.makedirs(address)
        catalog_buffer = self.read_catalog()
        catalog_buffer[table_name] = table 
        self.write_catalog(catalog_buffer)
        
    def drop_data(self, table_name):
        """
        删除data文件
        """
        pass

    def insert_data(self, table_name, value_list):
        """
        插入一条数据数据
        value_list  列表，值的列表
        """
        catalog_buffer = self.read_catalog()
        current_table = catalog_buffer[table_name]

        address = {}
        address['base'] = 'db_files/data/' + table_name + '/' + str(current_table["data_num"]) + '.dat'
        file_size = os.path.getsize(address['base']) if os.path.exists(address['base']) else 0
        fmt = current_table["fmt"]
        fmt = fmt + '?'
        size = struct.calcsize(fmt)
        address['offset'] = str(file_size//size)
        if file_size + size > self.max_size:
            catalog_buffer[table_name]["data_num"] += 1
            current_table = catalog_buffer[table_name]
            address['base'] = 'db_files/data/' + table_name + '/' + str(current_table["data_num"]) + '.dat'
            address['offset'] = 0
            self.write_catalog(catalog_buffer)
        
        self.write_data(address, fmt, value_list)

        return address

    def delete_data(self, table_name, condition):
        pass

    def select_data(self, table_name, condition):
        pass