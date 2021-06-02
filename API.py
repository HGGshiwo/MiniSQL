import re
import time
from enum import IntEnum
from IndexManager import insert_index, new_table
from CatalogManager import Table


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


def api(share, user='root'):
    while True:
        command = input()
        if command == 'create':
            last_time = time.time()
            create_table(share, user, 'test', ['i', '1s'], ['i', 'a'], [True, False], 'i')
            print('successfully create table in ' + str(time.time() - last_time))
        elif command == 'insert':
            last_time = time.time()
            for i in range(100):
                insert(share, 'test', [i, 'a'])
            print('successfully insert in ' + str(time.time() - last_time))
        elif command == 'delete':
            pass
        elif command == 'select':
            pass
        elif command == 'quit':
            print('successfully quit ' + user)
            return
        else:
            pass


def write_log(operate, result):
    pass


def create_table(share, user, table_name, fmt_list, column_list, unique_list, primary_key):
    """
    table_name  表名称
    fmt_list     列表，每个字符串格式
    column_list 列表，属性的名称
    unique_list 列表，属性是否唯一
    primary_key 字符串
    """
    op = Operate.create_table
    # 开始语法检查
    catalog_buffer = share.catalog_info
    for name in list(catalog_buffer.keys()):
        if table_name == name:
            result = Error.table_name_duplicate
            write_log(op, result)
            return result

        appearance = []
        if name in appearance:
            result = Error.column_name_duplicate
            write_log(op, result)
            return result
        else:
            appearance.append(name)

    pa = re.compile(r'\d*(i+|s+|c+|f+|\?+)')
    for fmt in fmt_list:
        if pa.match(fmt) is None:
            result = Error.type_not_support
            write_log(op, result)
            return result

    if primary_key is None:
        primary_key = column_list[0]
    new_table(share, table_name, column_list, fmt_list, unique_list, primary_key)

    # privilege_info = read_json('privilege')
    # privilege_info.append({'table_name': table_name, 'user': user, 'wen': True, 'ren': True, 'is_owner': True})
    # share.privilege_info = privilege_info
    # 写入log文件
    result = Error.no_error
    write_log(op, result)
    return result


def delete(share, table_name, condition):
    """
    删除data：根据select_index，将对应的记录enable=0
    删除index：根据select_index，将节点的指针删除，然后循环删除
    整个过程和插入差不多
    """
    pass


def insert(share, table_name, value_list):
    """
    插入
    """
    op = Operate.insert
    # 插入前看表是否存在0
    for i in share.catalog_info.keys():
        if len(share.catalog_info[i]) != 0 and share.catalog_info[i][Table.table_name] == table_name:
            primary_key = share.catalog_info[i][Table.primary_key]
            insert_index(share, value_list, table_name, primary_key)
            index_list = share.catalog_info[i][Table.index_list]
            for index in list(index_list.keys()):
                if index == primary_key:
                    continue
                insert_index(share, value_list, table_name, index)
            return

    raise RuntimeError('表名为 ' + table_name + ' 的表不存在.')




