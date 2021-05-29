from BufferManager import new_buffer, read_buffer, unpin_page, fresh_buffer
from CatalogManager import read_catalog, fresh_catalog
from RecordManager import insert_record
import sys


def new_table(share,  table_name, column_list, fmt_list, unique_list, primary_key):
    """
    table_name: 表的名称
    column_list: 列表
    primary_key: 主键
    """
    if primary_key is None:
        primary_key = column_list[0]
    index_no = column_list.index(primary_key)

    catalog_info = read_catalog(share)
    # 修改catalog_manager
    fmt = ''
    for i in fmt_list:
        fmt = fmt + i
    page_no = new_root(share, True, fmt, index_no)
    index_list = {primary_key: page_no}
    table = {
        'column_list': column_list,
        'fmt_list': fmt_list,
        'index_list': index_list,
        'unique_list': unique_list,
        'page_header': page_no,
        'primary_key': primary_key
    }
    catalog_info[table_name] = table
    fresh_catalog(share, catalog_info)
    pass


def delete_table(share, table_name):
    """
    删除record文件
    """
    pass


def new_root(share, is_leaf, fmt, index_offset):
    """
    创建新的根节点，不会在catalog中修改，因此需要外部修改catalog
    会直接写入buffer_pool中，可以从buffer_pool中获得该页
    share           需要修改buffer_info
    if_leaf         是否是叶子
    fmt             新的根解码形式
    index_offset    新的根索引在value_list的位置
    """
    # 为tree增加一页
    page_no = new_buffer(share)
    page = {
        'next_page': -1,
        'current_page': page_no,
        'previous_page': -1,
        'parent': -1,
        'fmt_size': len(fmt),
        'index_offset': index_offset,
        'is_leaf': is_leaf,
        'fmt': fmt,
        'user_record': []
    }
    fresh_buffer(share, page_no, page)
    return page_no


def delete_tree(share, table_name):
    pass


def select_page(share, table_name, condition=None):
    """
    查找page
    """
    pass


def check(page):
    page_size = 4096
    size = sys.getsizeof(page['fmt'])
    size += 25
    size += sys.getsizeof(page['user_record'])
    if size > page_size:
        return True
    return False


def insert_index(share, value_list, table_name, index):
    """
    从叶子插入，然后保持树
    table_name         当前操作的表
    index              当前的索引是哪个
    value_list         一条记录
    """
    catalog_info = read_catalog(share)  # 此处加了一个锁
    table = catalog_info[table_name]
    page_no = table['index_list'][index]
    index_offset = table['column_list'].index(index)
    index_fmt = table['fmt_list'][index_offset] + 'i'  # 索引的解码方式，需要i
    page = read_buffer(share, page_no)  # 只读，不修改,因此不保护下面的
    while not page['is_leaf']:
        for i in range(len(page['user_record'])):
            if value_list[index_offset] > page['user_record'][i][0]:
                unpin_page(share, page)
                page_no = page['user_record'][i][1]
                page = read_buffer(share, page_no)  # 这边多加了一个锁
                break

    value_list = [value_list]
    # 用主键插入
    # page是当前操作的页，下一个循环前更新为下一个想分裂的页，这个最后写入
    # left_page, right_page是page分裂后的两页，下一个循环前写入
    page = insert_record(page, value_list, index_offset)
    is_full = check(page)

    while is_full:
        n = len(page['user_record'])
        user_record = page['user_record'][0:n//2]
        right_page_no = new_buffer(share)  # left是原来的
        left_page_no = page_no
        right_page = {
            'next_page': right_page_no,
            'current_page': page['current_page'],
            'parent': page['parent'],
            'fmt_size': page['fmt_size'],
            'index_offset': page['index_no'],
            'is_leaf': page['is_leaf'],
            'fmt': page['fmt'],
            'user_record': user_record,
            'page_header': -1
        }
        user_record = page.user_record[n//2:]
        left_page = {
            'next_page': page['next_page'],
            'current_page': right_page_no,
            'parent': page['parent'],
            'fmt_size': page['fmt_size'],
            'index_offset': page['index_no'],
            'is_leaf': page['is_leaf'],
            'fmt': page['fmt'],
            'user_record': user_record,
            'page_header': -1
        }

        # 当分裂的时候，如果新页是非叶节点，把新页的孩子指向新页
        if not right_page['is_leaf']:
            for record in right_page['user_record']:
                p = read_buffer(share, record[1])
                p['parent'] = right_page_no
                fresh_buffer(share, record[1], p)

        # 如果是根的分裂，创建一个新根
        if page['parent'] == -1:
            page_no = new_root(share, False, fmt=index_fmt, index_offset=index_offset)
            catalog_info[table_name]['index_list'][index] = page_no
            left_page['parent'] = page_no
            right_page['parent'] = page_no

            # 把两个孩子插入到新根中
            left_value_list = [left_page['user_record'][0][index_offset], left_page['current_page']]
            right_value_list = [right_page['user_record'][0][index_offset], right_page['current_page']]
            value_lists = [left_value_list, right_value_list]
            page = insert_record(page, value_lists, 0)
            is_full = check(page)
        else:
            page_no = page['parent']
            page = read_buffer(share, page_no)
            value_lists = [[right_page['user_record'][0][index_offset], right_page['current_page']]]
            page = insert_record(page, value_lists, 0)
            is_full = check(page)

        fresh_buffer(share, left_page_no, left_page)
        fresh_buffer(share, right_page_no, right_page)
    pass
    fresh_buffer(share, page_no, page)
    fresh_catalog(share, catalog_info)


def delete_root(share):
    pass
