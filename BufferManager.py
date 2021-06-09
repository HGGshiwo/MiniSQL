import struct
import json
import os
from enum import IntEnum
from multiprocessing import shared_memory


class Off(IntEnum):
    current_page = 0
    next_page = 4
    header = 8
    space_header = 12
    is_leaf = 16
    previous_page = 17
    parent = 21
    index_offset = 25
    fmt_size = 29
    fmt = 33


class CatOff(IntEnum):
    #  下面是catalog_info中使用的
    pid = 0
    heap_top = 1
    del_header = 2


class BufferManager(object):
    """
    内存管理类
    """
    def __init__(self):
        try:
            self.pool = shared_memory.SharedMemory(create=False, name='pool')
            self.addr_list = shared_memory.ShareableList(sequence=None, name='addr_list')
            self.dirty_list = shared_memory.ShareableList(sequence=None, name='dirty_list')
            self.refer_list = shared_memory.ShareableList(sequence=None, name='refer_list')
            self.occupy_list = shared_memory.ShareableList(sequence=None, name='occupy_list')
            self.buffer_info = shared_memory.ShareableList(sequence=None, name='buffer')
            self.catalog_list = shared_memory.ShareableList(sequence=None, name='catalog_list')
            self.catalog_occupy_list = shared_memory.ShareableList(sequence=None, name='catalog_occupy_list')
            self.table_list = {}
            for table in self.catalog_list:
                self.table_list[table] = shared_memory.ShareableList(sequence=None, name=table)
        except FileNotFoundError:
            s = [-1] * 255  # 缓存空间的页数
            self.pool = shared_memory.SharedMemory(create=True, name='pool', size=1044480)
            self.addr_list = shared_memory.ShareableList(sequence=s, name='addr_list')
            self.dirty_list = shared_memory.ShareableList(sequence=s, name='dirty_list')
            self.refer_list = shared_memory.ShareableList(sequence=s, name='refer_list')
            self.occupy_list = shared_memory.ShareableList(sequence=s, name='occupy_list')
            buffer_info = read_json('buffer')
            self.buffer_info = buffer_info['0']
            catalog_info = read_json('catalog')
            self.catalog_list = shared_memory.ShareableList(sequence=catalog_info['catalog_list'], name='catalog_list')
            self.table_list = {}
            for table in self.catalog_list:
                if table == -1:
                    continue
                self.table_list[table] = shared_memory.ShareableList(sequence=catalog_info[table], name=table)
            t = [-1]*1024  # 最多建1024张表
            self.catalog_occupy_list = shared_memory.ShareableList(sequence=t, name='catalog_occupy_list')

    def pin_page(self, page_no):
        """
        将指定的进程id加入到需求列表，并阻塞，直到该page被pin住
        :param page_no: 页号
        :return: None
        """
        index = self.addr_list.index(page_no)
        pid = os.getpid()
        while self.occupy_list[index] != pid:
            if self.occupy_list[index] == -1:
                self.occupy_list[index] = pid
        pass
        self.dirty_list[index] = True
        self.refer_list[index] += 1

    def unpin_page(self, page_no):
        """
        将一页移出
        :param page_no:
        :return:
        """
        index = self.addr_list.index(page_no)
        self.occupy_list[index] = -1

    def pin_buffer(self):
        """
        pin buffer_info，即pin住物理文件的空闲位置
        :return:
        """
        pid = os.getpid()
        while self.buffer_info[CatOff.pid] != pid:
            if self.buffer_info[CatOff.pid] == -1:
                self.buffer_info[CatOff.pid] = pid

    def unpin_buffer(self):
        self.buffer_info[CatOff.pid] = -1

    def find_space(self, page_no):
        """
        为page_no在pool申请空间， 并在addr_list中注册
        :param page_no: 想要放入的页号
        :return: page_no在pool中的地址
        """
        # 如果有空闲的位置
        if self.addr_list.count(-1) != 0:
            index = self.addr_list.index(-1)
            self.addr_list[index] = page_no
            return index

        # 如果没有空闲，则用时钟算法删除一页
        i = 0
        while True:
            if self.occupy_list[i] == -1:
                if self.addr_list[i] == -1:
                    self.addr_list[i] = page_no
                    return i
                elif self.refer_list[i] > 0:
                    self.refer_list[i] -= 1
                elif self.refer_list[i] == 0:
                    # 删除这一页
                    pid = os.getpid()
                    self.occupy_list[i] = pid
                    index = i << 12
                    page = self.pool.buf[index:index+4096]
                    unload_buffer(page)
                    self.occupy_list[i] = -1
                    self.addr_list[i] = page_no
                    return i
            i = 0 if i == 255 else i + 1

    def load_page(self, page_no):
        """
        把文件加载入缓存
        :param page_no: 指定的页号
        :return: 返回写入的地址
        """
        index = self.find_space(page_no)
        index = index * 4096
        address = 'db_files/' + str(page_no) + '.dat'
        with open(address, 'rb') as file:
            page = file.read()
        length = len(page)
        self.pool[index:index + length] = page
        return index

    def new_buffer(self):
        """
        申请文件和对应的page_no, 将其加入缓存中，得到一个addr，将其pin住
        :param self:
        :return: 返回申请得到的地址addr, 页号page_no
        """
        # 计算page_no的位置
        self.pin_buffer()
        page_no = self.buffer_info[CatOff.del_header]
        if page_no == -1:  # 如果delete_list是空，则在heap中开辟
            self.buffer_info[CatOff.heap_top] += 1
            page_no = self.buffer_info[CatOff.heap_top]
            addr = self.find_space(page_no)
            self.pin_page(page_no)
            address = 'db_files/' + str(page_no) + '.dat'
            with open(address, 'a'):
                pass

        else:  # 如果delete_list非空
            addr = self.load_page(page_no)
            self.pin_page(page_no)
            index = addr << 12
            index += Off.next_page
            next_page = struct.unpack_from('i', self.pool.buf, index)[0]
            self.buffer_info[CatOff.del_header] = next_page

        self.unpin_buffer()
        return addr, page_no

    def delete_buffer(self, page_no):
        """
         将缓存中的page_no删除，将文件从文件链表中删除
        :param self:
        :param page_no:
        :return:
        """
        self.pin_page(page_no)
        self.pin_buffer()
        index = self.addr_list.index(page_no)
        index = index << 12
        index += Off.next_page
        next_page = self.buffer_info[Off.space_header]
        struct.pack_into('i', self.pool.buf, index, next_page)
        self.buffer_info[2] = page_no
        unload_buffer(page_no)
        self.unpin_page(page_no)


def unload_buffer(page):
    """
    将一个page写回文件
    :param page: 列表
    :return: None
    """
    page_no = struct.unpack_from('i', page, Off.current_page)[0]
    address = 'db_files/' + str(page_no) + '.dat'
    with open(address, 'wb') as file:
        file.write(page)


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

