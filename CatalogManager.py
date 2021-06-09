from enum import IntEnum
from BufferManager import BufferManager
import os


class Table(IntEnum):
    column_list = 0
    fmt_list = 1
    index_list = 2
    unique_list = 3
    page_header = 4
    primary_key = 5
    table_name = 6


class CatalogManager(BufferManager):
    def __init__(self):
        BufferManager.__init__(self)

    def pin_catalog(self, table_name):
        pid = os.getpid()
        index = self.catalog_list.index(table_name)
        while self.catalog_occupy_list[index] != pid:
            if self.catalog_occupy_list[index] == -1:
                self.catalog_occupy_list[index] = pid

    def unpin_catalog(self, table_name):
        index = self.catalog_list.index(table_name)
        self.catalog_occupy_list[index] = -1


    def pin_user(self, user_name):
        pass

    def unpin_user(self, user_name):
        pass

