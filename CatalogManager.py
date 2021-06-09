from BufferManager import pin_page, unpin_page
from enum import IntEnum


class Table(IntEnum):
    column_list = 0
    fmt_list = 1
    index_list = 2
    unique_list = 3
    page_header = 4
    primary_key = 5
    table_name = 6


def read_catalog(share, table_name):
    """
    读table_name对应的info，提供锁
    :param share:
    :param table_name:
    :return: table信息的列表
    """
    pin_page(share, 'catalog.' + table_name)
    for i in share.catalog_info.keys():
        if share.catalog_info[i][Table.table_name] == table_name:
            table_info = share.catalog_info[i]
            return list(table_info)


def fresh_catalog(share, table_name, table):
    for i in share.catalog_info.keys():
        if len(share.catalog_info[i]) != 0 and share.catalog_info[i][Table.table_name] == table_name:
            del share.catalog_info[i][0:]
            share.catalog_info[i].extend(table)
            unpin_page(share, 'catalog.'+ table_name)
            return

    for i in share.catalog_info.keys():
        if len(share.catalog_info[i]) == 0:
            share.catalog_info[i].extend(table)
            unpin_page(share, 'catalog.' + table_name)
            return

    unpin_page(share, 'catalog.' + table_name)
    raise RuntimeError('表数量已达到上限100，无法建表',)

