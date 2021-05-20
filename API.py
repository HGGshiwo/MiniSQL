import re
import time
from enum import IntEnum 
from threading import Thread
from Index_Manager import Index_Manager
from Catalog_Manager import Catalog_Manager, Fmt_List, Column
from Thread_Manager import Request, State
from Buffer_Manager import Buffer_Manager

class Error(IntEnum):
    no_error = 0
    table_name_duplicate = 1
    table_name_not_exists = 2
    column_name_duplicate = 3
    column_name_not_exists = 4
    type_not_support = 5
    user_not_exist = 6
    password_not_correct = 7


class Operate(IntEnum):
    create_table = 0
    insert = 1


class Api(Index_Manager):
    """
    提供用户接口
    """
    def __init__(self, user='root'):
        Index_Manager.__init__(self)
        self.current_user = user

    def write_log(self, operate, result):
        pass

    def create_table(self, table_name, fmt_list, column_list, primary_key):
        """
        table_name  表名称
        fmt         字符串格式
        column_list 字典
        primary_key 字符串
        """
        # with Request(table_name, State.read):
        op = Operate.create_table
        # 开始语法检查
        catalog_buffer = Catalog_Manager.catalog_buffer
        for name in list(catalog_buffer.keys()):
            if table_name == name:
                result = Error.table_name_duplicate
                self.write_log(op, result)
                return result

            appearance = []
            if name in appearance:
                result = Error.column_name_duplicate
                self.write_log(op, result)
                return result
            else:
                appearance.append(name)

        pa = re.compile(r'[^((\d*)(i|s|c|f)|(\?))]')
        if not (pa.match(fmt_list.fmt) is None):  # 匹配除了需要字符之外的字符
            result = Error.type_not_support
            self.write_log(op, result)
            return result

        if primary_key is None:
            primary_key = column_list.keys[0]
        self.new_table(table_name, column_list, fmt_list, primary_key)

        privilege_buffer = self.read_json('privilege')
        privilege_buffer[self.current_user] = {table_name: {'wen': True, 'ren': True, 'is_owner': True}}
        self.write_json('privilege', privilege_buffer)
        # 写入log文件
        result = Error.no_error
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
        op = Operate.insert
        # 插入前看表是否存在
        catalog_buffer = Catalog_Manager.catalog_buffer

        if table_name not in catalog_buffer.keys():
            result = Error.table_name_not_exists
            self.write_log(op, result)
            return result
        primary_key = Catalog_Manager.catalog_buffer[table_name].primary_key
        self.insert_index(value_list, table_name, primary_key)
        index_list = Catalog_Manager.catalog_buffer[table_name].index_list
        for index in list(index_list.keys()):
            if index == primary_key:
                continue
            self.insert_index(value_list, table_name, index)


# def test1():
#     """
#     测试点
#     1.建表
#     2.插入少量顺序记录
#     3.记录写入文件
#     4.文件读取
#     """
#     pass
#     a = Api('user1')
#     name_list = ['index', 'a']
#     unique = [True, False]
#     column_list = {}
#     for i in range(len(name_list)):
#         column = Column(unique[i], i)
#         column_list[name_list[i]] = column
#
#     last_time = time.time()
#     fmt_list = Fmt_List(('1i','1s'))
#
#     a.create_table('test', fmt_list, column_list, 'index')
#     print("create table in " + str(time.time()-last_time))
#
#     for i in range(100):
#         last_time = time.time()
#         value_list = [i, 'a']
#         e = a.insert('test', value_list)
#         print('user1:insert ' + str(i) + ' in ' + str(time.time()-last_time))
#     pool = Buffer_Manager.buffer_pool
#     a.unload_buffer(0)
#     pool = Buffer_Manager.buffer_pool
#     page = a.read_buffer(0)
#     pass
def test2():
    """
    测试点
    1.测试分裂节点
    """
    pass
    a = Api('user1')
    a.max_size = 20
    name_list = ['index', 'a']
    unique = [True, False]
    column_list = {}
    for i in range(len(name_list)):
        column = Column(unique[i], i)
        column_list[name_list[i]] = column

    last_time = time.time()
    fmt_list = Fmt_List(('1i','1s'))

    a.create_table('test', fmt_list, column_list, 'index')
    print("create table in " + str(time.time()-last_time))

    for i in range(100):
        last_time = time.time()
        value_list = [i, 'a']
        e = a.insert('test', value_list)
        print('user1:insert ' + str(i) + ' in ' + str(time.time()-last_time))

    pool = Buffer_Manager.buffer_pool
    pass


if __name__ == "__main__":
    catalog_manager = Catalog_Manager()
    # Thread(target=thread_user1).start()
    test2()
    # test_buffer()
    catalog_manager.sys_exit()
