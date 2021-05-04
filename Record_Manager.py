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
        '''
        table_name: 表的名称
        column_list: dict{name, is_unique}
        primary_key: 主键
        '''
        if(primary_key == None):
            primary_key = column_list.keys[0]
        
        #开始更新catalog_buffer[table_name]
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
        catalog_manager.catalog_buffer[table_name] = table 
        self.write_index('db_files/catalog.json', catalog_manager.catalog_buffer)
        
    def drop_data(self, table_name):
        '''
        删除data文件
        '''
        pass

    def insert_data(self, table_name, value_list):
        '''
        插入一条数据数据
        '''        
        current_table = self.catalog_buffer[table_name]
        column_list = list(current_table["column_list"].keys())
        data = {}
        for i, column in enumerate(column_list):
            data[column] = value_list[i]
        
        address = 'db_files/data/' + table_name + '/' + str(current_table["data_num"]) + '.dat'
        file_size = os.path.getsize(address) if os.path.exists(address) else 0
        fmt = current_table["fmt"]
        fmt = fmt + '?'
        size = struct.calcsize(fmt)
        if(file_size + size > self.max_size):
            catalog_manager.catalog_buffer[table_name]["data_num"] += 1
            current_table = catalog_manager.catalog_buffer[table_name]
            address ='db_files/data/' + table_name + '/' + str(current_table["data_num"]) + '.dat'
            self.write_index('db_files/catalog.json', catalog_manager.catalog_buffer)
        
        self.write_data(address, fmt, data)
        file_size = os.path.getsize(address) 
        return address + ":" + str(file_size//size)

    def delete_data(self, table_name, condition):
        pass

    def select_data(self, table_name, condition):
        pass