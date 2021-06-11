import re
import struct
import time
from enum import IntEnum
from IndexManager import IndexManager
from BufferManager import Off, write_json
from IndexManager import TabOff
from RecordManager import RecOff


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


class Api(IndexManager):
    def __init__(self):
        IndexManager.__init__(self)

    def print_info(self):
        print("pool", end='\t\t')
        print(self.pool.buf)
        print('addr_list', end='\t\t')
        print(self.addr_list)
        print('occupy_list', end='\t\t')
        print(self.occupy_list)
        print('delete_list', end='\t\t')
        print(self.delete_list)
        print('catalog_list', end='\t')
        print(self.catalog_list)

    def print_header(self, addr):
        print('------------------addr ' + str(addr) + ' info------------------')
        current_page = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.current_page)[0]
        print("current_page\t" + str(current_page))
        next_page = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.next_page)[0]
        print("next_page\t\t" + str(next_page))
        header = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]
        print("header\t\t\t" + str(header))
        is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]
        print("is_leaf\t\t\t" + str(is_leaf))
        previous_page = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.previous_page)[0]
        print("previous_page\t" + str(previous_page))
        fmt_size = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.fmt_size)[0]
        print("fmt_size\t\t" + str(fmt_size))
        fmt = struct.unpack_from(str(fmt_size) + 's', self.pool.buf, (addr << 12) + Off.fmt)[0]
        print('fmt\t\t\t\t' + str(fmt, encoding='utf8'))

    def print_record(self, addr):
        header = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]
        fmt_size = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.fmt_size)[0]
        fmt = struct.unpack_from(str(fmt_size) + 's', self.pool.buf, (addr << 12) + Off.fmt)[0]
        p = header
        while p != 0:
            pre = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.pre_addr)[0]
            r = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
            cur = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.curr_addr)[0]
            next = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
            valid = struct.unpack_from('?', self.pool.buf, (addr << 12) + p + RecOff.valid)[0]
            print("valid:" + str(valid) + "\tcur:" + str(cur) + "\tpre:" + str(pre) + '\tnext:' + str(next) + '\tr:', end='')
            print(r)
            p = next

    def create_table(self, table_name, primary_key, table_info):
        """
        创建一个表
        :param table_name: 表名
        :param primary_key: 主键位于第几个元素
        :param table_info: (name, fmt, unique, index_page)，其中index_page全部是-1
        :return:
        """
        self.new_table(table_name, primary_key, table_info)

    def delete(self, table_name, condition):
        """
        删除满足condition的节点
        :param table_name:
        :param condition:
        :return:
        """
        if self.catalog_list.count(table_name) != 0:
            delete_list = self.select_page(table_name, condition)  # 返回对应的记录
            table = self.table_list[table_name]
            for delete_record in delete_list:
                catalog_num = (len(table) - 2) // 4
                for i in range(0, catalog_num):
                    if table[(i << 2) + 5] != -1:
                        self.delete_index(table_name, i, delete_record[i])
            return
        print('表名为 ' + table_name + ' 的表不存在.')

    def insert(self, table_name, value_list):
        """
        插入一条数据
        :param table_name:
        :param value_list:
        :return:
        """
        if self.catalog_list.count(table_name) != 0:
            table = self.table_list[table_name]
            primary_key = table[TabOff.primary_key]
            # 在主索引树插入
            self.insert_index(value_list, table_name, primary_key)
            # 在二级索引树插入
            catalog_num = (len(table)-2) // 4
            for i in range(0, catalog_num):
                if i != primary_key and table[(i << 2) + 5] != -1:
                    index_value = [value_list[primary_key], value_list[i]]
                    self.insert_index(index_value, table_name, i)
            return

        print('表名为 ' + table_name + ' 的表不存在.')

    def select(self, table_name, condition):
        pass

    def create_index(self, table_name, index):
        pass

    def drop_index(self, table_name, index):
        pass

    def quit(self):
        for i in range(len(self.addr_list)):
            if self.dirty_list[i] and self.addr_list[i] != -1:
                self.unload_buffer(i)
        write_json('buffer', {"delete_list": list(self.delete_list)})
        catalog_info = {}
        catalog_info["catalog_list"] = list(self.catalog_list)
        for table in self.catalog_list:
            if table != -1:
                catalog_info[table] = list(self.table_list[table])
        write_json('catalog', catalog_info)
        print("退出成功.")
        pass
