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