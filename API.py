import re
from threading import Thread
import time
from enum import IntEnum 

from Buffer_Manager import main_buffer_manager
from Record_Manager import record_manager
from Index_Manager import index_manager
from Catalog_Manager import main_catalog_manager


class error(IntEnum):
    no_error = 0
    table_name_duplicate = 1
    table_name_not_exists = 2
    column_name_duplicate = 3
    column_name_not_exists = 4
    type_not_support = 5
    user_not_exist = 6
    password_not_correct = 7


class operation(IntEnum):
    create_table = 0
    insert = 1


class api(record_manager, index_manager):
    """
    提供用户接口
    """
    def __init__(self):
        self.current_user = 'root'
        record_manager.__init__(self)
        index_manager.__init__(self)

    def write_log(self, operation, result):
        pass

    def create_table(self, table_name, fmt, column_list, primary_key):
        """
        table_name  表名称
        fmt         字符串格式
        column_list 字典
        primary_key 字符串
        """
        op = operation.create_table
        # 开始语法检查
        catalog_buffer = self.read_catalog()
        for name in list(catalog_buffer.keys()):
            if table_name == name:
                result = error.table_name_duplicate
                self.write_log(op, result)
                return result 
            
            appearance = []
            if name in appearance:
                result = error.column_name_duplicate
                self.write_log(op, result)
                return result
            else:
                appearance.append(name)
        
        pa = re.compile(r'[^((\d*)(i|s|c|f)|(\?))]')
        if not (pa.match(fmt) is None):  # 匹配除了需要字符之外的字符
            result = error.type_not_support
            self.write_log(op, result)
            return result

        if primary_key is None:
            primary_key = column_list.keys[0]
        
        self.create_data(table_name, column_list, fmt, primary_key)
        self.create_tree(table_name, primary_key)
        
        privilege_buffer = self.read_index("db_files/privilege.json")
        privilege_buffer[self.current_user] = {table_name: {'wen': True, 'ren': True, 'is_owner': True}}
        self.write_index('db_files/privilege.json', privilege_buffer)
        # 写入log文件
        result = error.no_error
        self.write_log(op, result)
        return result
    
    def delete(self, table_name, condition):
        """
        删除data：根据select_index，将对应的记录enable=0
        删除index：根据select_index，将节点的指针删除，然后循环删除
        整个过程和插入差不多
        """
        pass

    def insert(self, table_name, value_list):
        """
        插入
        """
        op = operation.insert
        # 插入前看表是否存在
        catalog_buffer = self.read_catalog()
        if table_name not in catalog_buffer.keys():
            result = error.table_name_not_exists
            self.write_log(op, result)
            return result  
        
        catalog_buffer = self.read_catalog()
        current_table = catalog_buffer[table_name]
        address = self.insert_data(table_name, value_list)
        
        for i, index in enumerate(list(current_table["index_list"].keys())):
            index_value = value_list[i]
            self.insert_index(table_name, index, index_value, address)
        result = error.no_error
        return result


def buffer_thread():
    m = main_buffer_manager()


def catalog_tread():
    c = main_catalog_manager()


def thread1():
    pass
    a = api()
    column_list = {"index": True, "a": False}
    last_time = time.time()
    a.create_table('test', '1i1s', column_list, 'index')
    print("create table in " + str(time.time()-last_time))
    for i in range(100):
        last_time = time.time()
        value_list = [i, 'a']
        e = a.insert('test', value_list)
        print(str(i) + ":insert in " + str(time.time()-last_time))

    pass


if __name__ == "__main__":
    Thread(target=buffer_thread).start()
    Thread(target=catalog_tread).start()
    Thread(target=thread1).start()
    time.sleep(200)
    main_buffer_manager.is_quit = True
