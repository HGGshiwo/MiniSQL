import struct
import json
import os
from enum import IntEnum


class Page(IntEnum):
    next_page = 0
    current_page = 1
    previous_page = 2
    parent = 3
    fmt_size = 4
    index_offset = 5
    is_leaf = 6
    fmt = 7
    is_delete = 8
    is_change = 9
    reference = 10
    user_record = 11


def fresh_buffer(share, page, index):
    """
    在index对应的页修改为page, 取消page_no的锁
    :param share:
    :param page:
    :param index: 写入的地址
    :return: None
    """
    page_no = page[Page.current_page]
    del share.buffer_pool[index][0:]
    share.buffer_pool[index].extend(page)
    unpin_page(share, page_no)


def pin_page(share, page_no):
    """
    将指定的进程id加入到需求列表，并阻塞，直到该page被pin住
    :param share: 环境变量
    :param page_no: 页号
    :return: None
    """
    pid = os.getpid()
    share.request_pool[pid] = page_no
    if page_no not in share.pin_list:
        share.request_pool.pop(pid)
    while pid in share.request_pool.keys():
        pass  # 阻塞
    share.pin_list.append(page_no)

def unpin_page(share, page_no):
    """
    将一页从pin_list中移出
    :param share:
    :param page_no:
    :return:
    """
    share.pin_list.remove(page_no)


def load_buffer(page_no):
    """
    将文件加载为一个页然后返回, 不提供锁
    :param page_no: 指定的页号
    :return: 返回一个列表，代表页
    """
    address = 'db_files/' + str(page_no) + '.dat'
    with open(address, 'rb') as file:
        # 首先解码出除了fmt以外的部分
        header_fmt = '6i?'
        page_header = file.read(struct.calcsize(header_fmt))
        page_header = list(struct.unpack_from(header_fmt, page_header, 0))
        # 解码除了fmt_size之后，可以解码fmt
        fmt_size = struct.calcsize(str(page_header[Page.fmt_size]) + 's')
        fmt = file.read(fmt_size)
        fmt = struct.unpack_from(str(page_header[Page.fmt_size]) + 's', fmt, 0)
        page_header.append(fmt[0])
        user_buffer = file.read()

    records = struct.iter_unpack(fmt[0], user_buffer)
    user_record = []
    for record in records:
        record = to_string(record)
        user_record.append(record)
    page = []
    page.extend(page_header)
    page.extend([False, True])
    page.append(user_record)
    return page


def unload_buffer(page):
    """
    将一个page写回文件
    :param page: 列表
    :return: None
    """
    header_fmt = '6i?' + str(page[Page.fmt_size]) + 's'
    header_size = struct.calcsize(header_fmt)
    header_buffer = bytearray(header_size)
    fmt = to_bytes(page[Page.fmt])[0]
    page_header = page[0: Page.is_leaf + 1]
    page_header.append(fmt)
    struct.pack_into(header_fmt, header_buffer, 0, *page_header)
    size = struct.calcsize(page[Page.fmt])
    user_record = page[Page.user_record]
    user_buffer = bytearray(size * len(user_record))
    for i, record in enumerate(user_record):
        record = to_bytes(record)
        struct.pack_into(page[Page.fmt], user_buffer, i * size, *record)
    address = 'db_files/' + str(page[Page.current_page]) + '.dat'
    with open(address, 'wb') as file:
        file.write(header_buffer)
        file.write(user_buffer)


def find_space(share):
    """
    找一个空闲的位置，如果没有则删除，最后返回地址i
    :param share: 共享变量
    :return: buffer_pool中的地址
    """
    i = 0
    while True:
        if share.buffer_pool[i][Page.current_page] not in share.pin_list:
            if share.buffer_pool[i][Page.is_delete]:
                return i
            elif share.buffer_pool[i][Page.reference] > 0:
                share.buffer_pool[i][Page.reference] -= 1
            elif share.buffer_pool[i][Page.reference] == 0:
                # 删除这一页
                share.buffer_pool[i][Page.is_delete] = True
                page = share.buffer_pool[i]
                unload_buffer(page)
                return i
        i = 0 if i == 1023 else i + 1


def find_page(share, page_no):
    """
    找到page_no对应的索引i
    :param share:
    :param page_no:
    :return: 索引i, 找不到返回-1
    """
    for i in range(len(share.buffer_pool)):
        if share.buffer_pool[i][Page.current_page] == page_no:
            return i
    return -1


def read_buffer(share, page_no):
    """
    对应页数的读取,提供锁
    :param share:
    :param page_no:
    :return:page 列表
    """
    pin_page(share, page_no)
    index = find_page(share, page_no)
    if index == -1:
        page = load_buffer(page_no)
        index = find_space(share)
        fresh_buffer(share, page, index)
    return list(share.buffer_pool[index])


def new_buffer(share):
    """
    开辟一个新的文件，提供锁
    :param share:
    :return: 返回page_no
    """
    # 计算page_no的位置
    pin_page(share, 'buffer_info')
    buffer_info = share.buffer_info
    page_no = buffer_info['del_header']
    if page_no == -1:  # 如果delete_list是空，则在heap中开辟
        buffer_info['heap_top'] += 1
        page_no = buffer_info['heap_top']
        pin_page(share, page_no)
        address = 'db_files/' + str(page_no) + '.dat'
        with open(address, 'a'):
            pass
    else:  # 如果delete_list非空
        pin_page(share, page_no)
        index = find_page(share, page_no)
        buffer_info['del_header'] = share.buffer_pool[index][Page.next_page]

    share.buffer_info = buffer_info
    unpin_page(share, 'buffer_info')
    return page_no


def delete_buffer(share, page_no):
    """
     要求必须在缓存中，文件从链表删除，提供锁
    :param share:
    :param page_no:
    :return:
    """
    pin_page(share, page_no)
    index = find_page(share, page_no)
    buffer_info = share.buffer_info
    share.buffer_pool[index][Page.next_page] = buffer_info['del_header']
    share.buffer_pool[index][Page.is_delete] = True
    buffer_info['del_header'] = page_no
    share.buffer_info = buffer_info
    unload_buffer(page_no)
    unpin_page(share, page_no)


def read_json(json_name):
    """
    读json文件，只发生在catalog类初始时，实例化对象时候不会读文件，注意不入缓存池
    """
    address = 'db_files/' + json_name + '.json'
    with open(address, 'r') as file:
        buffer = json.load(file)
    return buffer


def write_json(json_name, buffer):
    """
    写json文件，只发生在系统退出前，实例化对象时不会读文件
    """
    buffer = json.dumps(buffer, ensure_ascii=False)
    address = 'db_files/' + json_name + '.json'
    with open(address, 'w') as file:
        file.write(buffer)


def to_bytes(value):
    value = list(value)
    for i in range(len(value)):
        if isinstance(value[i], str):
            value[i] = str.encode(value[i])
    return value


def to_string(value):
    value = list(value)
    for i in range(len(value)):  # 对于每一个属性.str转为bytes
        if isinstance(value[i], bytes):
            value[i] = str(value[i], encoding="utf-8")
    return value
