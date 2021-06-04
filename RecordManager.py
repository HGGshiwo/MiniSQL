import struct
from CatalogManager import CatalogManager
from BufferManager import Off
from enum import IntEnum


class RecOff(IntEnum):
    valid = 0
    curr_addr = 1
    next_addr = 5
    pre_addr = 9
    record = 13


class RecordManager(CatalogManager):
    def __init__(self):
        CatalogManager.__init__(self)

    def insert_record(self, addr, record):
        """
        在指定的index的页中插入一条数据
        :param addr:
        :param record:插入的数据
        :return:返回is_full如果已满则返回false，否则返回True
        """
        index_offset = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.index_offset)[0]
        fmt_size = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.fmt_size)[0]
        fmt = struct.unpack_from(str(fmt_size)+'s', self.pool.buf, (addr << 12) + Off.fmt)[0]
        fmt = str(fmt, encoding="utf-8")

        # offset是物理地址相对addr的偏移
        offset = fmt_size + Off.fmt
        valid = struct.unpack_from('?', self.pool.buf, (addr << 12) + offset)[0]  # 是否有效
        gap = struct.calcsize('?3i' + fmt)
        while valid:
            offset = offset + gap
            if offset + gap > 4096:
                return True
            valid = struct.unpack_from('?', self.pool.buf, (addr << 12) + offset)[0]
        for i, item in enumerate(record):
            if isinstance(item, str):
                record[i] = str.encode(item)
        # 在物理地址插入
        struct.pack_into(fmt, self.pool.buf, (addr << 12) + offset + RecOff.record, *record)

        # 找到逻辑地址
        p = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]
        q = p
        if p == 0:  # 是第一条记录
            struct.pack_into('i', self.pool.buf, (addr << 12) + Off.header, offset)
        else:
            while p != 0: # p是r的后一条指针，q是r的指针对应的记录
                q = p
                r = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + q + RecOff.record)
                if r[index_offset] > record[index_offset]:
                    break
                p = struct.unpack_from('i', self.pool.buf, (addr << 12) + q + RecOff.next_addr)[0]
            struct.pack_into('i', self.pool.buf, (addr << 12) + q + RecOff.next_addr, offset)

        # 插入在r的后面
        struct.pack_into('?', self.pool.buf, (addr << 12) + offset + RecOff.valid, True)
        struct.pack_into('3i', self.pool.buf, (addr << 12) + offset + RecOff.curr_addr, offset, p, q)
        return False

    def delete_record(self, addr, condition):
        """
        删除一条记录
        :param addr: 页的地址
        :param condition:
        :return:None
        """
        offset = addr << 12
        fmt_size = struct.unpack_from('i', self.pool.buf, offset + Off.fmt_size)[0]
        fmt = struct.unpack_from(str(fmt_size) + 's', self.pool.buf, offset + Off.fmt)[0]
        p = struct.unpack_from('i', self.pool.buf, offset + Off.header)[0]  # p是指针
        while p != 0:
            r = struct.unpack_from(fmt, self.pool.buf, offset + p + RecOff.record)
            if true(r, condition):  # 如果条件成立
                struct.pack_into('?', self.pool.buf, offset + p + RecOff.valid)
                pre_addr = struct.unpack_from(fmt, self.pool.buf, offset + p + RecOff.pre_addr)[0]
                next_addr = struct.unpack_from(fmt, self.pool.buf, offset + p + RecOff.next_addr)[0]
                struct.pack_into('i', self.pool.buf, offset + next_addr, pre_addr)
            p = struct.unpack_from('i', self.pool.buf, offset + p + RecOff.next_addr)[0]
        pass

    def select_record(self, addr, condition):  # 线性查找
        """
        在一个page中寻找record
        """
        offset = addr << 12
        fmt_size = struct.unpack_from('i', self.pool.buf, offset + Off.fmt_size)[0]
        fmt = struct.unpack_from(str(fmt_size) + 's', self.pool.buf, offset + Off.fmt)[0]
        record_list = []
        p = struct.unpack_from('i', self.pool.buf, offset + Off.header)[0]  # p是指针
        while p != 0:
            r = struct.unpack_from(fmt, self.pool.buf, offset + p + RecOff.record)
            if true(r, condition):  # 如果条件成立
                record_list.append(r)
                break
            p = struct.unpack_from('i', self.pool.buf, offset + p + RecOff.next_addr)[0]

        return record_list


def true(record, condition):
    return True
    pass
