from BufferManager import new_buffer, read_buffer, unpin_page, fresh_buffer, find_space, Page, pin_page, find_page
from CatalogManager import read_catalog, fresh_catalog, Table
from RecordManager import insert_record
import sys


def new_table(share,  table_name, column_list, fmt_list, unique_list, primary_key):
    """
    为新表格在catalog中注册
    :param share:
    :param table_name: 表的名称
    :param column_list: 列表
    :param fmt_list: 列表
    :param unique_list: 列表
    :param primary_key: 主键
    :return: None
    """
    if primary_key is None:
        primary_key = column_list[0]
    index_no = column_list.index(primary_key)

    pin_page(share, 'catalog.' + table_name)
    # 修改catalog_manager
    fmt = ''
    for i in fmt_list:
        fmt = fmt + i
    page_no = new_root(share, True, fmt, index_no)
    index_list = {primary_key: page_no}
    table = [column_list, fmt_list, index_list, unique_list, page_no, primary_key, table_name]
    fresh_catalog(share, table_name, table)
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
    :param share: 需要修改buffer_info
    :param is_leaf: 是否是叶子
    :param fmt: 新的根解码形式
    :param index_offset: 新的根索引在value_list的位置
    :return:新根所在的page_no
    """
    # 为tree增加一页
    page_no = new_buffer(share)
    page = [-1, page_no, -1, -1, len(fmt), index_offset, is_leaf, fmt, False, True, 1, []]
    index = find_space(share)
    fresh_buffer(share, page, index)
    return page_no


def true(item, condition):
    pass


def delete_tree(share, table_name,condition=None):
    """
    删除所搜寻的索引
    """
    table = read_catalog(share, table_name)
    page_no = table[Table.page_header]
    page = read_buffer(share, page_no)
    while not page[Page.isleaf]:#如果当前的page不是叶节点，那么循环到叶节点
        #page=page.child，即循环到孩子节点
        #i = 0
        #while i < len(page.user_record) - 1:
        #    i += 1
        page_no = page[Page.user_record][0][1]#到最左边的一页
        unpin_page(share, page_no)
        page = read_buffer(share, page_no)
    #现在循环到了最左侧的叶子节点，现在在叶子节点当中查询。
    while(page[Page.next_page]):#不是最右边的那一页
        user_record=page[Page.user_record]
        for item in user_record:
            if true(item,condition):#如果item符合condition
                item=None
    unpin_page(share, 'catalog.' + table_name)


def select_page(share, table_name, condition=None):
    """
    查找page**********************************
    从根节点开始，如果符合条件那么返回来符合条件的索引；如果不符合条件那么继续进行
    """
    table_info = read_catalog(share, table_name)
    page_no=table_info[Table.page_header]
    page=read_buffer(share, page_no)
    while not page[Page.isleaf]:#如果当前的page不是叶节点，那么循环到叶节点
        #page=page.child，即循环到孩子节点
        #i = 0
        #while i < len(page.user_record) - 1:
        #    i += 1
        unpin_page(share, page_no)
        page_no = page[Page.user_record][0][1]#到最左边的一页
        page = read_buffer(share, page_no)
    #现在循环到了最左侧的叶子节点，现在在叶子节点当中查询。
    indexlist=[]
    while page[Page.next_page] != -1:#不是最右边的那一页
        user_record=page[Page.user_record]
        for item in user_record:
            if true(item,condition):#如果item符合condition
                indexlist.append(item)
    return indexlist


def check(page):
    """
    检测page是不是满了
    :param page: 列表
    :return: True或者False
    """
    page_size = 4096
    size = sys.getsizeof(page[Page.fmt])
    size += 25
    size += sys.getsizeof(page[Page.user_record])
    if size > page_size:
        return True
    return False


def insert_index(share, value_list, table_name, index):
    """
    从叶子插入，然后保持树
    :param share:
    :param value_list: 一条记录
    :param table_name: 当前操作的表
    :param index: 当前的索引是哪个
    :return: None
    """
    table = read_catalog(share, table_name)  # 此处加了一个锁
    page_no = table[Table.index_list][index]
    index_offset = table[Table.column_list].index(index)
    index_fmt = table[Table.fmt_list][index_offset] + 'i'  # 索引的解码方式，需要i
    page = read_buffer(share, page_no)  # 只读，不修改,因此不保护下面的
    while not page[Page.is_leaf]:
        for i in range(len(page[Page.user_record])):
            if value_list[index_offset] > page[Page.user_record][i][0]:
                unpin_page(share, page_no)
                page_no = page[Page.user_record][i][1]
                page = read_buffer(share, page_no)  # 这边多加了一个锁
                break

    value_list = [value_list]
    # 用主键插入
    # page是当前操作的页，下一个循环前更新为下一个想分裂的页，这个最后写入
    # left_page, right_page是page分裂后的两页，下一个循环前写入
    page = insert_record(page, value_list, index_offset)
    is_full = check(page)

    while is_full:
        n = len(page[Page.user_record])
        user_record = page[Page.user_record][0:n//2]
        right_page_no = new_buffer(share)  # left是原来的
        left_page_no = page_no
        right_page = [right_page_no, page[Page.current_page], page[Page.parent], page[Page.fmt_size],
                      page[Page.index_offset], page[Page.is_leaf], page[Page.fmt], False, True, 1, user_record]
        user_record = page[Page.user_record][n//2:]
        left_page = [page[Page.next_page], page_no, page[Page.parent],
                     page[Page.fmt_size], page[Page.index_offset], page[Page.is_leaf],
                     page[Page.fmt], False, True, page[Page.reference], user_record]

        # 当分裂的时候，如果新页是非叶节点，把新页的孩子指向新页
        if not right_page[Page.is_leaf]:
            for record in right_page[Page.user_record]:
                p = read_buffer(share, record[1])
                p[Page.parent] = right_page_no
                fresh_buffer(share, record[1], p)

        # 如果是根的分裂，创建一个新根
        if page[Page.parent] == -1:
            page_no = new_root(share, False, fmt=index_fmt, index_offset=index_offset)
            table[Table.index_list][index] = page_no
            left_page[Page.parent] = page_no
            right_page[Page.parent] = page_no

            # 把两个孩子插入到新根中
            left_value_list = [left_page[Page.user_record][0][index_offset], left_page[Page.current_page]]
            right_value_list = [right_page[Page.user_record][0][index_offset], right_page[Page.current_page]]
            value_lists = [left_value_list, right_value_list]
            page = insert_record(page, value_lists, 0)
            is_full = check(page)
        else:
            page_no = page[Page.parent]
            page = read_buffer(share, page_no)
            value_lists = [[right_page[Page.user_record][0][index_offset], right_page[Page.current_page]]]
            page = insert_record(page, value_lists, 0)
            is_full = check(page)

        fresh_buffer(share, left_page_no, left_page)
        fresh_buffer(share, right_page_no, right_page)
    pass
    index = find_page(share, page_no)
    fresh_buffer(share, page, index)
    fresh_catalog(share, table_name, table)


def delete_root(share):
    pass
