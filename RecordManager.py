from BufferManager import read_buffer


def insert_record(page, value_lists, index_no):
    """
    在指定的页插入一条数据,如果需要分裂，返回False
    page_no         需要插入的页
    value_list      需要插入的值的列表
    index_no        索引是value_list的第几个
    """
    user_record = page['user_record']
    if len(user_record) == 0:
        value_list = value_lists.pop()
        user_record.append(value_list)
    for value_list in value_lists:
        for i in range(len(user_record) + 1):
            if i == len(user_record) or value_list[index_no] <= user_record[i][index_no]:
                user_record.insert(i, value_list)
                break
    page['user_record'] = user_record
    return page


def delete_record(share, page_no, offset):
    read_buffer(share, page_no)
    buffer_pool = share.buffer_pool
    buffer_pool[page_no].pop(offset)
    share.buffer_pool = buffer_pool
    share.pin_list.remove(page_no)
