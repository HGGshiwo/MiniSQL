import struct
from enum import IntEnum
from IndexManager import IndexManager
from BufferManager import Off
from CatalogManager import CatalogManager
from RecordManager import RecOff
import re


class TabOff(IntEnum):
    primary_key = 0
    leaf_header = 1
    name = 0
    fmt = 1
    unique = 2
    index_page = 3


class Api(IndexManager, CatalogManager):
    def __init__(self, lock_list=None):
        self.grant_list = []

        if lock_list is not None:
            IndexManager.__init__(self, lock_list[256:])
            CatalogManager.__init__(self, lock_list[0:256])
        else:
            IndexManager.__init__(self)
            CatalogManager.__init__(self)

    def print_info(self):
        print("pool", end='\t\t')
        print(self.pool.buf)
        print('addr_list', end='\t\t')
        print(self.addr_list)
        print('file_list', end='\t\t')
        print(self.file_list)

    def create_table(self, table_name, primary_key, table_info):
        """
        为新表格在catalog中注册
        :param table_name:
        :param primary_key:
        :param table_info: 顺序是name, fmt, unique, index_page全部是-1
        :return:
        """
        # 为该表开辟文件空间存储
        fmt = ''
        for i in range(len(table_info)):
            if i % 4 == 1:
                fmt = fmt + table_info[i]
        page_no = self.new_root(True, fmt)
        self.new_table(table_name, primary_key, table_info, page_no)
        pass

    def delete(self, table_name, condition):
        """
        删除满足condition的节点
        :param table_name:
        :param condition:
        :return:
        """
        table = self.get_table(table_name)
        delete_list = self.select(table_name, condition)  # 返回对应的记录

        for delete_record in delete_list:
            catalog_num = (len(table) - 2) // 4
            for i in range(0, catalog_num):
                if table[(i << 2) + 5] != -1:
                    page_no = self.table_list[table_name][2 + (i >> 2) + TabOff.index_page]  # 根节点会随时改
                    index_fmt = 'i' + str(table[2 + (i >> 2) + TabOff.fmt])
                    leaf_header, index_page = self.delete_index(page_no, i, index_fmt, delete_record[i])
                    if leaf_header is not None:
                        self.table_list[table_name][TabOff.leaf_header] = leaf_header
                    if index_page is not None:
                        self.table_list[table_name][(i << 2) + 5] = index_page
        return

    def insert(self, table_name, value_list):
        """
        插入一条数据
        :param table_name:
        :param value_list:
        :return:
        """
        table = self.get_table(table_name)
        primary_key = table[TabOff.primary_key]

        # 在主索引树插入
        page_no = table[2 + (primary_key << 2) + TabOff.index_page]  # 根节点
        # 索引的解码方式，页号+索引值
        index_fmt = 'i' + str(table[2 + (primary_key << 2) + TabOff.fmt])
        new_root = self.insert_index(value_list, page_no, primary_key, index_fmt)
        if new_root != -1:
            self.table_list[table_name][2 + (primary_key << 2) + TabOff.index_page] = new_root

        # 在二级索引树插入
        catalog_num = (len(table)-2) // 4
        for i in range(0, catalog_num):
            if i != primary_key and table[(i << 2) + 5] != -1:
                index_fmt = 'i' + str(table[2 + (i << 2) + TabOff.fmt])
                value = [value_list[i], value_list[primary_key]]
                page_no = table[2 + (i << 2) + TabOff.index_page]
                new_root = self.insert_index(value, page_no, i, index_fmt)
                if new_root != -1:
                    self.table_list[table_name][2 + (i << 2) + TabOff.index_page] = new_root
        return

    def select(self, table_name, cond_list):
        """
        直接进行查找，二级索引叶子中r[0]是索引值，r[1]是主键值
        :param cond_list:
        :param table_name:
        :return:
        """
        table = self.get_table(table_name)
        primary_key = table[TabOff.primary_key]
        primary_page = table[(primary_key << 2) + 5]  # 主索引所在根
        primary_index_fmt = 'i' + table[(primary_key << 2) + 3]
        # 命令预处理
        index_cond = None
        primary_cond = None
        page_no = -1  # 索引的根
        index_fmt = None
        for i, cond in enumerate(cond_list):
            cond = re.match(r'^([A-Za-z0-9_]+)\s*([<>=]+)\s*(.+)$', cond, re.S)
            column_name, op, value = cond.groups()

            # 将name转为数字
            column = (table.index(column_name) - 2) // 4
            # 将value转为数字
            try:
                value = int(value)
            except ValueError:
                pass
            try:
                value = float(value)
            except ValueError:
                pass

            cond_list[i] = [column, op, value]

            index_page = table.index(column_name) + 3
            fmt = table.index(column_name) + 1
            if cond_list[i][1] != '<>':  # 不等于也是遍历全部
                if table[index_page] != -1:
                    if column == primary_key:
                        primary_cond = cond_list[i]
                        page_no = table[index_page]
                    elif index_cond is None:
                        index_cond = cond_list[i]
                        index_cond[0] = 0  # 在二级索引树，索引的位置变成了0
                        page_no = table[index_page]
                        index_fmt = 'i' + table[fmt]

        ret = []
        # 如果是主索引查询，首先到根
        if primary_cond is not None:
            ret = self.select_page(primary_page, primary_index_fmt, primary_cond, cond_list)
        elif index_cond is not None:
            primary_value = self.select_page(page_no, index_fmt, index_cond, cond_list)
            for value in primary_value:
                primary_cond = [primary_key, '=', value[1]]
                page_ret = self.select_page(primary_page, primary_index_fmt, primary_cond, [primary_cond])
                ret.extend(page_ret)
        else:
            leaf_header = table[TabOff.leaf_header]
            ret = self.liner_select(leaf_header, cond_list)
        return ret

    def create_index(self, table_name, index):
        """
        创建一颗树
        :param table_name:
        :param index:
        :return:
        """
        table = self.get_table(table_name)
        res = []
        # res[]会按照[INDEX_VALUE,PRIMARY_KEY_VALUE]存储数据，调用函数排序，NEW_ROOT,不断调用插入，返回root
        leaf_header = table[TabOff.leaf_header]  # leaf_header是第一个叶子节点所在的页号
        page_no = leaf_header
        col_num = (len(table) - 2) // 4
        fmt = ''
        for i in range(col_num):
            fmt = fmt + table[(i << 2) + 3]
        primary_key = table[TabOff.primary_key]

        if table[(index << 2) + 5] != -1:
            raise Exception('I1')
        elif table[(index << 2) + 4] is not True:
            raise Exception('I2')
        if primary_key == index:
            raise Exception('I3')

        sec_fmt = table[(index << 2) + 3] + table[(primary_key << 2) + 3]
        sec_index_fmt = 'i' + table[(index << 2) + 3]

        while page_no != -1:
            addr = self.get_addr(page_no)
            p = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]
            while p != 0:
                r = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
                res.append([r[index], r[primary_key]])
                p = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
            page_no = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.next_page)[0]

        page_no = self.create_tree(res, sec_fmt, sec_index_fmt)
        self.table_list[table_name][(index << 2) + 5] = page_no

    def drop_index(self, table_name, index):
        """
        删除指定索引
        :param table_name:
        :param index:
        :return:
        """
        table = self.get_table(table_name)
        page_no = table[(index << 2) + 5]
        fmt = table[(index << 2) + 3]
        index_fmt = 'i' + fmt
        self.drop_tree(page_no, index_fmt)
        self.table_list[table_name][(index << 2) + 5] = -1

    def drop_table(self, table_name):
        """
        删除一个表
        :param table_name:
        :return:
        """
        table = self.get_table(table_name)
        col_num = (len(table) - 1)//4
        for i in range(col_num):
            page_no = table[(i << 2) + 5]
            fmt = table[(i << 2) + 3]
            index_fmt = 'i' + fmt
            if page_no != -1:
                self.drop_tree(page_no, index_fmt)
        self.delete_table(table_name)
        return

    def quit(self):
        self.quit_catalog()
        self.quit_buffer()
        print("退出成功.")
        pass
