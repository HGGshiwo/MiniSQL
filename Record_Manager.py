import struct
import sys

from Catalog_Manager import Catalog_Manager
from Buffer_Manager import Buffer_Manager


class Record_Manager(Catalog_Manager):
    """
    主要是为了维护每一页中记录的链表
    """
    def __init__(self):
        self.max_size = 4*1024

    def insert_record(self, page_no, value_lists, index_no):
        """
        在指定的页插入一条数据,如果需要分裂，返回False
        page_no         需要插入的页
        value_list      需要插入的值的列表
        index_no        索引是value_list的第几个
        """
        page = self.read_buffer(page_no)
        if len(page.user_record) == 0:
            value_list = value_lists.pop(0)
            page.user_record.append(value_list)

        for value_list in value_lists:
            i = 0
            while i < len(page.user_record):
                if value_list[index_no] <= page.user_record[i][index_no]:
                    break
                i += 1
            page.user_record.insert(i, value_list)
        Buffer_Manager.buffer_pool[page_no] = page
        if sys.getsizeof(page.page_header) + len(page.user_record) * struct.calcsize(page.fmt) > self.max_size:
            return True
        return False

    def delete_record(self, table_name, condition):
        pass

    def select_record(self, table_name, condition):
        """
        在一个page中寻找record
        """
        pass
