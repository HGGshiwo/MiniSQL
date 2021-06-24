import struct
import json
from enum import IntEnum
from multiprocessing import shared_memory


class LockOff(IntEnum):
    addr_list = 256
    dirty_list = 257
    refer_list = 258
    file_list = 259


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
    def __init__(self, lock_list):
        self.lock_list = lock_list
        try:
            self.pool = shared_memory.SharedMemory(create=False, name='pool')
            self.addr_list = shared_memory.ShareableList(sequence=None, name='addr_list')
            self.dirty_list = shared_memory.ShareableList(sequence=None, name='dirty_list')
            self.refer_list = shared_memory.ShareableList(sequence=None, name='refer_list')
            self.file_list = shared_memory.ShareableList(sequence=None, name='file_list')
        except FileNotFoundError:
            s = [-1] * 256  # 缓存空间的页数
            self.pool = shared_memory.SharedMemory(create=True, name='pool', size=(2 << 20))
            self.addr_list = shared_memory.ShareableList(sequence=s, name='addr_list')
            self.dirty_list = shared_memory.ShareableList(sequence=s, name='dirty_list')
            self.refer_list = shared_memory.ShareableList(sequence=s, name='refer_list')
            address = 'db_files/buffer.json'
            with open(address, 'r') as file:
                buffer = json.load(file)
            file_list = buffer['file_list']
            self.file_list = shared_memory.ShareableList(sequence=file_list, name='file_list')

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
        addr = self.get_addr(page_no)
        self.file_list[page_no] = -1
        self.addr_list[addr] = -1

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

    def get_addr(self, page_no):
        """
        寻找page_no的地址
        :param page_no:
        :return: addr
        """
        if self.addr_list.count(page_no) == 0:
            self.load_page(page_no)
        return self.addr_list.index(page_no)

    def quit_buffer(self):
        for i in range(len(self.addr_list)):
            if self.dirty_list[i] and self.addr_list[i] != -1:
                self.unload_buffer(i)
        self.pool.close()

        buffer = json.dumps({"file_list": list(self.file_list)}, ensure_ascii=False)
        address = 'db_files/buffer.json'
        with open(address, 'w') as file:
            file.write(buffer)
        self.file_list.shm.close()
        self.addr_list.shm.close()
        self.refer_list.shm.close()
        self.dirty_list.shm.close()
