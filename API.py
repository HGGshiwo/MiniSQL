import struct
import time
from enum import IntEnum
from IndexManager import IndexManager, TabOff
from BufferManager import Off, write_json
from RecordManager import RecOff
from InterpreterManager import InterpreterManager


class Error(IntEnum):
    no_error = 0
    table_name_duplicate = 1
    table_name_not_exists = 2
    column_name_duplicate = 3
    column_name_not_exists = 4
    type_not_support = 5
    user_not_exist = 6
    password_not_correct = 7


class Api(IndexManager, InterpreterManager):
    def __init__(self, lock_list=None):
        IndexManager.__init__(self)
        self.lock_list = lock_list

    def print_info(self):
        print("pool", end='\t\t')
        print(self.pool.buf)
        print('addr_list', end='\t\t')
        print(self.addr_list)
        print('file_list', end='\t\t')
        print(self.file_list)
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
            next_addr = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
            valid = struct.unpack_from('?', self.pool.buf, (addr << 12) + p + RecOff.valid)[0]
            print("valid:" + str(valid) + "\tcur:" + str(cur)
                  + "\tpre:" + str(pre) + '\tnext:' + str(next_addr) + '\tr:', end='')
            print(r)
            p = next_addr

    def exe_cmd(self):
        """
        执行命令
        :return:
        """
        args = self.get_cmd()
        self.create_table(*args)
        pass

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
        """
        直接进行查找
        :param table_name:
        :param condition:
        :return:
        """
        return self.select_page(table_name, condition)
        pass

    def create_index(self, table_name, index_name):
        """
        新建一颗二级索引树
        树的叶节点为table中各节点的主键与值相对应index的值，
        按index值升序排列
        """
        if self.catalog_list.count(table_name) != 0:
            # 表存在
            res = []
            # res[]会按照[INDEX_VALUE,PRIMARY_KEY_VALUE]存储数据，调用函数排序，NEW_ROOT,不断调用插入，返回root
            table = self.table_list[table_name]
            # 找到table
            leaf_header = table[TabOff.leaf_header]
            # leaf_header是第一个叶子节点所在的页号
            page_no = leaf_header
            primary_key_location = table[TabOff.primary_key]
            index_location = -1
            catalog_num = (len(table) - 2) // 4
            for i in range(0, catalog_num + 1):
                if (i == catalog_num):
                    # 这个判断感觉不够巧妙
                    print("index_name not exist")
                    return
                if table[(i << 2) + 2] == index_name:
                    if (i == primary_key_location):
                        print("no need to create index on primary key")
                        return
                    if table[(i << 2) + 2 + 2] is True:
                        if (table[(i << 2) + 2 + 3] != -1):
                            print("index already created")
                        else:
                            index_location = i
                            break
                    else:
                        print("index not unique")
                        return

            while page_no != -1:
                if self.addr_list.count(page_no) == 0:
                    self.load_page(page_no)
                addr = self.addr_list.index(page_no)
                p = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]
                # 第一条记录的位置 [0]有什么用？？
                fmt_size = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.fmt_size)[0]
                fmt = struct.unpack_from(str(fmt_size) + 's', self.pool.buf, (addr << 12) + Off.fmt)[0]
                while p != 0:
                    valid = struct.unpack_from('?', self.pool.buf, (addr << 12) + p + RecOff.valid)[0]
                    # 记录是否有效
                    if valid:
                        r = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
                        # r的具体内容我不太清楚,借助primary_key_location , index_location合成需要的数据
                        # 类似[r[primary_key_location],r[index_location]]
                        res.append([r[primary_key_location], r[index_location]])
                    p = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
                    # p转到下一条记录

                page_no = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.next_page)
                # 转到下一个page
            # 升序排列
            res.sort(key=secondelement)
            # print(res)
            # 为该表开辟文件空间存储
            fmt = ''
            fmt = fmt + struct.unpack_from(str(fmt_size) + 's', self.pool.buf, (addr << 12) + Off.fmt)[
                primary_key_location]  # !!fmt_size addr可能未赋值
            fmt = fmt + struct.unpack_from(str(fmt_size) + 's', self.pool.buf, (addr << 12) + Off.fmt)[index_location]
            # 检验此fmt是否正确
            root_page_no = self.new_root(True, fmt)
            table[(i << 2) + 2 + 3] = root_page_no  # !! i可能未赋值
            # 修改table中的index信息
            for res_value in res:
                self.insert_index(res_value, table_name, index_location)
            return
        print('表名为 ' + table_name + ' 的表不存在.')

    def drop_index(self, table_name, index_name):
        """
        删除指定的一颗二级索引树
        和要求格式有所出入
        调用delete_buffer

        """
        if self.catalog_list.count(table_name) != 0:
            # 表存在
            table = self.table_list[table_name]
            # 找到table
            # leaf_header = -1
            page_no = -1  # nope
            primary_key_location = table[TabOff.primary_key]
            # index_location = -1
            catalog_num = (len(table) - 2) // 4
            for i in range(0, catalog_num + 1):
                if (i == catalog_num):
                    print("index_name not exist")
                    return
                if table[(i << 2) + 2] == index_name:
                    if (i == primary_key_location):
                        print("can't drop primary key")
                        return
                    if (table[(i << 2) + 2 + 3] != -1):  # 检查index_page
                        page_no = table[(i << 2) + 2 + 3]  # 找到index_page
                        # index_location = i
                        # leaf_header = page_no
                        table[(i << 2) + 2 + 3] = -1  # 索引清除，table中信息置为-1
                        break
                    else:
                        print("index not created")
                        return

            while page_no != -1:
                # self.addr_list.count(page_no) == 0: 的情况应该不存在 ，但是我不确定
                addr = self.addr_list.index(page_no)
                temp_page_no = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.next_page)
                self.delete_buffer(page_no)
                # 直接运用的现有函数，可能会有问题
                page_no = temp_page_no
                # 转到下一个page

            return
        print('表名为 ' + table_name + ' 的表不存在.')

    def drop_table(self, table_name):
        if self.catalog_list.count(table_name) != 0:
            # 表存在
            table = self.table_list[table_name]
            # 找到table
            leaf_header = table[TabOff.leaf_header]
            # leaf_header是第一个叶子节点所在的页号   , 那么它也是primary_key对应的页号
            page_no = leaf_header
            primary_key_location = table[TabOff.primary_key]
            catalog_num = (len(table) - 2) // 4
            for i in range(0, catalog_num):
                if (table[(i << 2) + 2 + 3] != -1 and i != primary_key_location):  # 若存在索引，不为主键
                    self.delete_index(table_name, table[(i << 2) + 2])  # !!使用错误
                elif (i == primary_key_location):  # 主键的情况,对应主索引树
                    table[(i << 2) + 2 + 3] = -1
                    while page_no != -1:
                        # self.addr_list.count(page_no) == 0: 的情况应该不存在 ，但是我不确定
                        addr = self.addr_list.index(page_no)
                        temp_page_no = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.next_page)
                        self.delete_buffer(page_no)
                        # 直接运用的现有函数，可能会有问题
                        page_no = temp_page_no
                        # 转到下一个page
            self.catalog_list[table_name] = -1
            return
        print('表名为 ' + table_name + ' 的表不存在.')

    def quit(self):
        for i in range(len(self.addr_list)):
            if self.dirty_list[i] and self.addr_list[i] != -1:
                self.unload_buffer(i)
        write_json('buffer', {"file_list": list(self.file_list)})
        catalog_info = {}
        catalog_info["catalog_list"] = list(self.catalog_list)
        for table in self.catalog_list:
            if table != -1:
                catalog_info[table] = list(self.table_list[table])
        write_json('catalog', catalog_info)
        print("退出成功.")
        pass


def firstelement(elem):
    return elem(0)


def secondelement(elem):
    return elem(1)
