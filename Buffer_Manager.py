import struct
import json
from Thread_Manager import Thread_Manager


class Page(object):
    """
    一页
    """
    def __init__(self, next_page=None, current_page=None, parent=None, fmt_size=None, index_no=None,
                 is_leaf=None, fmt=None, user_record=None, page_header=None, acquire_times=1):
        self.acquire_times = acquire_times
        if page_header is not None:
            self.next_page = page_header[0]
            self.current_page = page_header[1]
            self.parent = page_header[2]
            self.fmt_size = page_header[3]
            self.index_no = page_header[4]
            self.is_leaf = page_header[5]
            self.fmt = page_header[6]
            self.user_record = user_record
        else:
            self.next_page = next_page
            self.current_page = current_page
            self.parent = parent
            self.fmt_size = fmt_size
            self.index_no = index_no
            self.is_leaf = is_leaf
            self.fmt = fmt
            self.user_record = user_record

    @property
    def page_header(self):
        page_header = [self.next_page, self.current_page, self.parent, self.fmt_size,
                       self.index_no, self.is_leaf, self.fmt]
        return tuple(page_header)


class Buffer_Manager(Thread_Manager):
    """
    主要是寻找新的空间，管理缓存池
    """
    buffer_pool = {}
    pool_size = 1024  # 缓冲池最大页数量
    del_header = None  # 被删除的页的头节点
    heap_top = None  # 页的头节点，需要加锁访问

    def __init__(self):
        Thread_Manager.__init__(self)
        if Buffer_Manager.del_header is None:
            with open('db_files/buffer.json', 'r') as file:
                buffer = json.load(file)
            Buffer_Manager.del_header = buffer["del_header"]
            Buffer_Manager.heap_top = buffer['heap_top']

    def load_buffer(self, page_no):
        """
        将文件加载入缓存页，不提供锁
        """
        address = 'db_files/' + str(page_no) + '.dat'
        with open(address, 'rb') as file:
            # 首先解码出除了fmt以外的部分
            header_fmt = '5i?'
            header_buffer = file.read(struct.calcsize(header_fmt))
            page_header = list(struct.unpack_from(header_fmt, header_buffer, 0))
            # 解码除了fmt_size之后，可以解码fmt
            fmt_size = struct.calcsize(str(page_header[3])+'s')
            fmt_buffer = file.read(fmt_size)
            fmt = struct.unpack_from(str(page_header[3]) + 's', fmt_buffer, 0)
            page_header.append(fmt[0])
            user_buffer = file.read()

        records = struct.iter_unpack(fmt[0], user_buffer)
        user_record = []
        for record in records:
            record = to_string(record)
            user_record.append(record)
        page = Page(page_header=page_header, user_record=user_record)
        # 写入时如果已满，则删除缓存
        if len(Buffer_Manager.buffer_pool) == Buffer_Manager.pool_size:
            self.unload_buffer()

        Buffer_Manager.buffer_pool[page_no] = page

    def unload_buffer(self, page_no = None):
        """
        将一个缓存删除并写回文件，不提供锁
        """
        # 删除一个最不常访问的
        if page_no is None:
            min_times = int('inf')
            for p in list(Buffer_Manager.buffer_pool.keys()):
                if Buffer_Manager.buffer_pool[p]['acquire_times'] < min_times:
                    min_times = Buffer_Manager.buffer_pool[p]['acquire_times']
                    page_no = p

        # 删除一个指定的
        page = Buffer_Manager.buffer_pool.pop(page_no)
        header_fmt = '5i?' + str(page.fmt_size) + 's'
        header_size = struct.calcsize(header_fmt)
        header_buffer = bytearray(header_size)
        page_header = page.page_header
        page_header = to_bytes(page_header)
        struct.pack_into(header_fmt, header_buffer, 0, *page_header)
        size = struct.calcsize(page.fmt)
        user_buffer = bytearray(size * len(page.user_record))
        for i,record in enumerate(page.user_record):
            record = to_bytes(record)
            struct.pack_into(page.fmt, user_buffer, i*size, *record)
        print(user_buffer)
        address = 'db_files/' + str(page_no) + '.dat'
        with open(address, 'wb') as file:
            file.write(header_buffer)
            file.write(user_buffer)

    def read_buffer(self, page_no):
        """
        对应页数的读取
        """
        if page_no in Buffer_Manager.buffer_pool.keys():
            buffer = Buffer_Manager.buffer_pool[page_no]
            Buffer_Manager.buffer_pool[page_no].acquire_times += 1
        else:
            buffer = None

        if buffer is None:
            self.load_buffer(page_no)
            buffer = Buffer_Manager.buffer_pool[page_no]

        return buffer

    def new_buffer(self, page = None):
        """
        开辟一个新的文件，并返回page_no
        """
        page_no = Buffer_Manager.del_header
        # 如果delete_list是空，则在heap中开辟
        if page_no == -1:
            Buffer_Manager.heap_top += 1
            page_no = Buffer_Manager.heap_top
            address = 'db_files/' + str(page_no) + '.dat'
            with open(address, 'a'):
                pass
        # 如果delete_list非空
        else:
            if page_no not in Buffer_Manager.buffer_pool.keys():
                self.load_buffer(page_no)
            Buffer_Manager.del_header = Buffer_Manager.buffer_pool[page_no].next_page
        if page is not None:
            Buffer_Manager.buffer_pool[page_no].page = page
        return page_no

    def delete_buffer(self, page_no):
        """
        将一个缓存删除，文件从链表删除，不提供锁
        """
        page = Page(next_page=Buffer_Manager.del_header, current_page=page_no, parent=-1,
                    fmt_size=0, is_leaf=True, fmt='', user_record=[])
        Buffer_Manager.del_header = page_no
        Buffer_Manager.buffer_pool[page_no] = page
        self.unload_buffer(page_no)

    def read_json(self, json_name):
        """
        读json文件，只发生在catalog类初始时，实例化对象时候不会读文件，注意不入缓存池
        """
        address = 'db_files/' + json_name + '.json'
        with open(address, 'r') as file:
            buffer = json.load(file)
        return buffer

    def write_json(self, json_name, buffer):
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