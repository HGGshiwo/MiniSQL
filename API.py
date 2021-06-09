from os import stat_result
import re
import struct
import time
from enum import IntEnum
from IndexManager import IndexManager
from BufferManager import Off
from IndexManager import TabOff
from RecordManager import RecOff, RecordManager, delete_from_table, select, select_from_table

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
    select=2
    delete=3


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
        print('buffer_info', end='\t\t')
        print(self.buffer_info)
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
            pre = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + p + RecOff.pre_addr)[0]
            r = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
            next = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
            print("record:\tpre:" + str(pre) + '\tnext:' + str(next) + '\tr:', end='')
            print(r)
            p = next

    def create_table(self, table_name, primary_key, table_info):
        """
        table_name  表名称
        fmt_list     列表，每个字符串格式
        column_list 列表，属性的名称
        unique_list 列表，属性是否唯一
        primary_key 字符串
        """
        self.new_table(table_name, primary_key, table_info)

    def select(self,args):
        op=Operate.select
        args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
        lists = args.split(' ')
        start_from = re.search('from', args).start()
        end_from = re.search('from', args).end()
        columns = args[0:start_from].strip()
        if re.search('where', args):
            start_where = re.search('where', args).start()
            end_where = re.search('where', args).end()
            table = args[end_from+1:start_where].strip()
            conditions = args[end_where+1:].strip()
        else:
            table = args[end_from+1:].strip()
            conditions = ''
        #CatalogManager.catalog.not_exists_table(table)
        #CatalogManager.catalog.check_select_statement(table,conditions,columns)
        select_from_table(table,conditions,columns)
        
    def delete(self,args):
        """
        args:输入的数据
        删除data：根据select_index，将对应的记录enable=0
        删除index：根据select_index，将节点的指针删除，然后循环删除
        整个过程和插入差不多
        """
        op=Operate.delete
        args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
        lists = args.split(' ')
        if lists[0] != 'from':#delete from table_name where ......
            raise Exception("API Module : Unrecoginze symbol for command 'delete',it should be 'from'.")
        table_name = lists[1]
        statement=list[2:]
        delete_from_table(table_name,statement)
        # if self.catalog_list.count(table_name) != 0:
        #     table = self.table_list[table_name]
            
        #     primary_key = table[TabOff.primary_key]
        #     addr = self.delete_index(returnvalue, table_name, primary_key)
        #     catalog_num = (len(table)-2) // 4
        #     for i in range(0, catalog_num):
        #         if i != primary_key and table[(i << 2) + 5] != -1:
        #             index_value = [addr, returnvalue[i]]
        #             self.insert_index(index_value, table_name, i)
        #     return
        # raise RuntimeError('表名为 ' + table_name + ' 的表不存在.') 



    def insert(self, table_name, value_list):
        """
        插入
        """
        op = Operate.insert
        # 插入前看表是否存在0

        if self.catalog_list.count(table_name) != 0:
            table = self.table_list[table_name]
            primary_key = table[TabOff.primary_key]
            addr = self.insert_index(value_list, table_name, primary_key)
            catalog_num = (len(table)-2) // 4
            for i in range(0, catalog_num):
                if i != primary_key and table[(i << 2) + 5] != -1:
                    index_value = [addr, value_list[i]]
                    self.insert_index(index_value, table_name, i)
            return

        raise RuntimeError('表名为 ' + table_name + ' 的表不存在.')

    def exit(self):
        pass
