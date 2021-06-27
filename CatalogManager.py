from enum import IntEnum
from multiprocessing import shared_memory
import json


class Table(IntEnum):
    column_list = 0
    fmt_list = 1
    index_list = 2
    unique_list = 3
    page_header = 4
    primary_key = 5
    table_name = 6


class CatalogManager(object):
    """
    表管理类
    """
    def __init__(self, table_lock = None):
        try:
            self.table_lock = table_lock
            self.catalog_list = shared_memory.ShareableList(sequence=None, name='catalog_list')
            self.table_list = {}
            for table in self.catalog_list:
                if table == -1:
                    continue
                self.table_list[table] = shared_memory.ShareableList(sequence=None, name=table)
        except FileNotFoundError:
            address = 'db_files/catalog.json'
            with open(address, 'r') as file:
                catalog_info = json.load(file)
            self.catalog_list = shared_memory.ShareableList(sequence=catalog_info['catalog_list'], name='catalog_list')
            self.table_list = {}
            for table in self.catalog_list:
                if table == -1:
                    continue
                self.table_list[table] = shared_memory.ShareableList(sequence=catalog_info[table], name=table)

    def new_table(self, table_name, primary_key, table_info, page_no):
        """
        新建一个表的信息
        :param table_name: 表名
        :param primary_key: 主键在表中的位置
        :param table_info: 表的信息，按照“索引名、fmt、是否unique、元素索引所在根节点位置（非索引则为-1）”的顺序，逐个元素排列
        :param page_no: 储存表的页号
        :return:
        """
        if self.catalog_list.count(-1) == 0:
            raise Exception('B1')

        if self.catalog_list.count(table_name) == 1:
            raise Exception('T1')

        # 在catalog_list中注册该表
        index = self.catalog_list.index(-1)
        self.catalog_list[index] = table_name

        # 将该表的信息写入内存
        table = [primary_key, page_no]
        table = table + table_info
        table[(primary_key << 2) + 5] = page_no  # 为primary key的属性index_page赋值
        self.table_list[table_name] = shared_memory.ShareableList(sequence=table, name=table_name)

    def get_table(self, table_name):
        """
        获得表
        :param table_name:表名
        :return:表，即self.table_list[table_name]
        """
        if self.catalog_list.count(table_name) == 0:
            raise Exception('T2')
        return self.table_list[table_name]

    def delete_table(self, table_name):
        """
        删除表信息
        :param table_name:表名
        :return:None
        """
        table_index = self.catalog_list.index(table_name)
        self.catalog_list[table_index] = -1
        self.table_list[table_name].shm.unlink()
        return

    def quit_catalog(self):
        """
        退出表管理
        :return:None
        """
        catalog_info = {}
        catalog_info["catalog_list"] = list(self.catalog_list)
        for table in self.catalog_list:
            if table != -1:
                catalog_info[table] = list(self.table_list[table])
                self.table_list[table].shm.close()
        buffer = json.dumps(catalog_info, ensure_ascii=False)
        address = 'db_files/catalog.json'
        with open(address, 'w') as file:
            file.write(buffer)
        self.catalog_list.shm.close()

