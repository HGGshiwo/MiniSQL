import struct
import json
from enum import IntEnum
from multiprocessing import shared_memory


class LockOff(IntEnum):
    # 前255个是缓存
    # 后255个是表
    addr_list = 512
    dirty_list = 513
    refer_list = 514
    file_list = 515
    catalog_list = 516


class Off(IntEnum):
    current_page = 0
    next_page = 4
    header = 8
    is_leaf = 12
    previous_page = 13
    parent = 17
    fmt_size = 21
    fmt = 25


class BufferManager(object):
    """
    内存管理类
    """
    def __init__(self):
        self.lock_list = None
        self.grant_list = []
        try:
            self.pool = shared_memory.SharedMemory(create=False, name='pool')
            self.addr_list = shared_memory.ShareableList(sequence=None, name='addr_list')
            self.dirty_list = shared_memory.ShareableList(sequence=None, name='dirty_list')
            self.refer_list = shared_memory.ShareableList(sequence=None, name='refer_list')
            self.file_list = shared_memory.ShareableList(sequence=None, name='file_list')
            self.catalog_list = shared_memory.ShareableList(sequence=None, name='catalog_list')
            self.table_list = {}
            for table in self.catalog_list:
                self.table_list[table] = shared_memory.ShareableList(sequence=None, name=table)
        except FileNotFoundError:
            s = [-1] * 256  # 缓存空间的页数
            self.pool = shared_memory.SharedMemory(create=True, name='pool', size=(2 << 20))
            self.addr_list = shared_memory.ShareableList(sequence=s, name='addr_list')
            self.dirty_list = shared_memory.ShareableList(sequence=s, name='dirty_list')
            self.refer_list = shared_memory.ShareableList(sequence=s, name='refer_list')
            file_list = read_json('buffer')['file_list']
            self.file_list = shared_memory.ShareableList(sequence=file_list, name='file_list')
            catalog_info = read_json('catalog')
            self.catalog_list = shared_memory.ShareableList(sequence=catalog_info['catalog_list'], name='catalog_list')
            self.table_list = {}
            for table in self.catalog_list:
                if table == -1:
                    continue
                self.table_list[table] = shared_memory.ShareableList(sequence=catalog_info[table], name=table)

    def pin_page(self, addr):
        """
        将指定的缓存池中的地址加锁
        :param addr: 地址
        :return: None
        """
        if self.lock_list is not None:
            lock = self.lock_list[addr]
            if lock not in self.grant_list:
                lock.acquire()
                self.grant_list.append(lock)

    def pin_file(self):
        """
        将文件列表pin住
        :return:
        """
        if self.lock_list is not None:
            lock = self.lock_list[LockOff.file_list]
            if lock not in self.grant_list:
                lock.acquire()
                self.grant_list.append(lock)

    def pin_table(self, table_name):
        """
        将表pin住
        :return:
        """
        if self.lock_list is not None:
            self.pin_catalog()
            table = self.catalog_list.index(table_name)
            lock = self.lock_list[table << 2]
            if lock not in self.grant_list:
                lock.acquire()
                self.grant_list.append(lock)

    def pin_catalog(self):
        """
        将表pin住
        :return:
        """
        if self.lock_list is not None:
            lock = self.lock_list[LockOff.catalog_list]
            if lock not in self.grant_list:
                lock.acquire()
                self.grant_list.append(lock)

    def pin_dirty(self):
        """
        pin脏页表
        :return:
        """
        if self.lock_list is not None:
            lock = self.lock_list[LockOff.dirty_list]
            if lock not in self.grant_list:
                lock.acquire()
                self.grant_list.append(lock)

    def pin_refer(self):
        """
        pin引用表
        :return:
        """
        if self.lock_list is not None:
            lock = self.lock_list[LockOff.refer_list]
            if lock not in self.grant_list:
                lock.acquire()
                self.grant_list.append(lock)

    def release_lock(self):
        """
        释放所有锁
        :return:
        """
        for lock in self.grant_list:
            lock.release()

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
            if self.addr_list[i] == -1:
                self.addr_list[i] = page_no
                return i
            elif self.refer_list[i] > 0:
                self.refer_list[i] -= 1
            elif self.refer_list[i] == 0:
                # 删除这一页
                self.unload_buffer(i)
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
        self.pool.buf[index:index + length] = page[:]
        return index

    def new_buffer(self):
        """
        申请文件和对应的page_no, 将其加入缓存中，得到一个addr，将其pin住
        :param self:
        :return: 返回申请得到的地址addr, 页号page_no
        """
        # 计算page_no的位置
        if self.file_list.count(-1) == 0:
            raise Exception('B1')
        page_no = self.file_list.index(-1)
        self.file_list[page_no] = 0
        addr = self.find_space(page_no)
        address = 'db_files/' + str(page_no) + '.dat'
        with open(address, 'a'):
            pass
        return addr, page_no

    def delete_buffer(self, page_no):
        """
         将缓存中的page_no删除，将文件删除
        :param self:
        :param page_no:
        :return:
        """
        addr = self.addr_list.index(page_no)
        self.file_list[page_no] = -1
        self.unload_buffer(addr)

    def unload_buffer(self, addr):
        """
        将一个page写回文件
        :param addr: 地址
        :return: None
        """
        page_no = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.current_page)[0]
        address = 'db_files/' + str(page_no) + '.dat'
        buf = bytearray(4096)
        buf[:] = self.pool.buf[(addr << 12): ((addr + 1) << 12)]
        with open(address, 'wb') as file:
            file.write(buf)


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
