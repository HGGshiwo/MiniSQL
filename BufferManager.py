import struct
import json
import os


def fresh_buffer(share, page_no, page):
    buffer_pool = share.buffer_pool
    buffer_pool[page_no] = page
    share.buffer_pool = buffer_pool
    unpin_page(share, page_no)


def pin_page(share, page_no):
    pid = os.getpid()
    share.request_pool[pid] = page_no
    while pid in share.request_pool.keys():
        pass  # 阻塞


def unpin_page(share, page_no):
    share.pin_list.remove(page_no)


def load_buffer(page_no):
    """
    将文件加载为一个页然后返回, 不提供锁
    """
    address = 'db_files/' + str(page_no) + '.dat'
    with open(address, 'rb') as file:
        # 首先解码出除了fmt以外的部分
        header_fmt = '6i?'
        page_header = file.read(struct.calcsize(header_fmt))
        page_header = list(struct.unpack_from(header_fmt, page_header, 0))
        # 解码除了fmt_size之后，可以解码fmt
        fmt_size = struct.calcsize(str(page_header[4]) + 's')
        fmt = file.read(fmt_size)
        fmt = struct.unpack_from(str(page_header[4]) + 's', fmt, 0)
        page_header.append(fmt[0])
        user_buffer = file.read()

    records = struct.iter_unpack(fmt[0], user_buffer)
    user_record = []
    for record in records:
        record = to_string(record)
        user_record.append(record)
    page = {
        'next_page': page_header[0],
        'current_page': page_header[1],
        'previous_page': page_header[2],
        'parent': page_header[3],
        'fmt_size': page_header[4],
        'index_offset': page_header[5],
        'is_leaf': page_header[6],
        'fmt': page_header[7],
        'user_record': user_record
    }
    return page


def unload_buffer(page):
    """
    将一个page写回文件
    """
    header_fmt = '6i?' + str(page['fmt_size']) + 's'
    header_size = struct.calcsize(header_fmt)
    header_buffer = bytearray(header_size)
    fmt = to_bytes([page['fmt']])[0]
    page_header = (page['next_page'], page['current_page'], page['previous_page'],
                   page['parent'], page['fmt_size'], page['index_offset'], page['is_leaf'], fmt)
    struct.pack_into(header_fmt, header_buffer, 0, *page_header)
    size = struct.calcsize(page['fmt'])
    user_record = page['user_record']
    user_buffer = bytearray(size * len(user_record))
    for i, record in enumerate(user_record):
        record = to_bytes(record)
        struct.pack_into(page['fmt'], user_buffer, i * size, *record)
    address = 'db_files/' + str(page['current_page']) + '.dat'
    with open(address, 'wb') as file:
        file.write(header_buffer)
        file.write(user_buffer)


def free_pool(share):
    """
    如果满的时候调用，使用时钟置换法
    """
    while True:
        for page_no in share.buffer_pool.keys():
            if share.buffer_pool[page_no] > 0:
                share.buffer_pool[page_no] -= 1
            if share.buffer_pool[page_no] == 0 and page_no not in share.pin_list:
                # 删除这一页
                page = share.buffer_pool.pop(page_no)
                unload_buffer(page)
                return


def read_buffer(share, page_no):
    """
    对应页数的读取,提供锁
    """
    pool_size = 1024
    pin_page(share, page_no)
    if page_no not in share.buffer_pool.keys():
        if len(share.buffer_pool) == pool_size:
            free_pool(share)
        page = load_buffer(page_no)
        fresh_buffer(share, page_no, page)
    return share.buffer_pool[page_no]


def new_buffer(share):
    """
    开辟一个新的文件，并返回page_no，提供锁
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
        buffer_info['del_header'] = share.buffer_pool[page_no]['next_page']

    share.buffer_info = buffer_info
    unpin_page(share, 'buffer_info')
    return page_no


def delete_buffer(share, page_no):
    """
    将一个缓存删除，文件从链表删除，提供锁
    """
    pin_page(share, page_no)
    user_record = []
    page = {
        'next_page': share.buffer_info['del_header'],
        'current_page': page_no,
        'parent': -1,
        'fmt_size': 0,
        'is_leaf': True,
        'fmt': '',
        'user_record': user_record
    }
    buffer_info = share.buffer_info
    buffer_info['del_header'] = page_no
    share.buffer_info = buffer_info
    buffer_pool = share.buffer_pool
    buffer_pool[page_no] = page
    share.buffer_pool = buffer_pool
    unload_buffer(page_no)


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
    buffer = json.dumps(buffer, indent=4, ensure_ascii=False)
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
