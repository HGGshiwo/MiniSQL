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

    def insert_record(self, addr, record, index):
        """
        在指定的index的页中插入一条数据，如果已满，则删除最后一条数据，然后插入新数据，然后返回

        :param index: 以第几个属性为索引进行插入
        :param addr:
        :param record:插入的数据
        :return:返回 head_value, record
                分别表示该节点头需要修改的值，该节点最后一条记录。
                如果不需要修改，也未满，则返回None
        """
        head_value = None
        last_record = None

        fmt_size = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.fmt_size)[0]
        fmt = struct.unpack_from(str(fmt_size)+'s', self.pool.buf, (addr << 12) + Off.fmt)[0]
        fmt = str(fmt, encoding="utf-8")

        # offset是物理地址相对addr的偏移
        offset = fmt_size + Off.fmt
        valid = struct.unpack_from('?', self.pool.buf, (addr << 12) + offset + RecOff.valid)[0]  # 是否有效
        gap = struct.calcsize('3i')
        gap += struct.calcsize('?')
        gap += struct.calcsize(fmt)
        while valid:
            offset = offset + gap
            if offset + gap > 4096:
                # 非常麻烦，需要删除最后一条记录，然后将最后一条记录作为新记录返回
                # 这么做的目的是，无法确定该条记录在新页还是旧页中，只能替换成一条确定的记录
                # 去寻找最后一条记录
                p = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]
                offset = p
                while p != 0:
                    offset = p
                    p = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
                last_record = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + offset + RecOff.record)
                pre_addr = struct.unpack_from('i', self.pool.buf, (addr << 12) + offset + RecOff.pre_addr)[0]
                struct.pack_into('i', self.pool.buf, (addr << 12) + pre_addr + RecOff.next_addr, 0)
                break
            valid = struct.unpack_from('?', self.pool.buf, (addr << 12) + offset + RecOff.valid)[0]

        for i, item in enumerate(record):
            if isinstance(item, str):
                record[i] = str.encode(item)
        # 在物理地址插入
        struct.pack_into(fmt, self.pool.buf, (addr << 12) + offset + RecOff.record, *record)

        # 找到逻辑地址
        p = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]
        q = 0  # q是r的前一条
        if p == 0:  # 是第一条记录
            struct.pack_into('i', self.pool.buf, (addr << 12) + Off.header, offset)
        else:
            while p != 0:  # q是r的前一个，p是r的指针
                r = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
                if r[index] > record[index]:
                    break
                q = p
                p = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
            if q == 0:
                # 如果前面一条不存在，则修改头
                struct.pack_into('i', self.pool.buf, (addr << 12) + Off.header, offset)
                head_value = record[index]
            else:
                # 如果前面一条存在，把前面一条的next赋值为offset
                struct.pack_into('i', self.pool.buf, (addr << 12) + q + RecOff.next_addr, offset)
            if p != 0:
                # 如果后面一条存在，则指向offset
                struct.pack_into('i', self.pool.buf, (addr << 12) + p + RecOff.pre_addr, offset)
        # 修改这条记录自己的值
        struct.pack_into('?', self.pool.buf, (addr << 12) + offset + RecOff.valid, True)
        struct.pack_into('3i', self.pool.buf, (addr << 12) + offset + RecOff.curr_addr, offset, p, q)

        return head_value, last_record

    def delete_record(self, addr, index, value):
        """
        删除指定索引值的记录

        :param value: 索引值是多少
        :param index: 索引是哪一个
        :param addr: 页的地址
        :return:should_merge，是否需要合并，head_value, 当前第一条记录的值，如果不需要修改返回None
        """
        head_value = None
        fmt_size = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.fmt_size)[0]
        fmt = struct.unpack_from(str(fmt_size) + 's', self.pool.buf, (addr << 12) + Off.fmt)[0]
        p = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]  # p是指针
        while p != 0:
            r = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
            if r[index] == value:  # 如果条件成立
                struct.pack_into('?', self.pool.buf, (addr << 12) + p + RecOff.valid, False)
                pre_addr = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + p + RecOff.pre_addr)[0]
                next_addr = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
                if next_addr != 0:
                    struct.pack_into('i', self.pool.buf, (addr << 12) + next_addr + RecOff.pre_addr, pre_addr)
                if pre_addr != 0:
                    struct.pack_into('i', self.pool.buf, (addr << 12) + pre_addr + RecOff.next_addr, next_addr)
                else:
                    next_record = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + next_addr + RecOff.record)
                    head_value = next_record[index]
                    struct.pack_into('i', self.pool.buf, (addr << 12) + Off.header, next_addr)
            p = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
        # 通过物理地址检测是否需要合并
        valid_num, invalid_num = self.count_valid(addr)
        half_empty = True if (valid_num < invalid_num) else False
        return half_empty, head_value

    def count_valid(self, addr):
        """
        统计一页中有几条有效记录
        :param addr:
        :return:valid_num, invalid_num
        """
        valid_num = 0
        invalid_num = 0
        fmt_size = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.fmt_size)[0]
        fmt = struct.unpack_from(str(fmt_size) + 's', self.pool.buf, (addr << 12) + Off.fmt)[0]
        fmt = str(fmt, encoding='utf8')
        p = fmt_size + Off.fmt
        gap = struct.calcsize('?')
        gap += struct.calcsize('3i')
        gap += struct.calcsize(fmt)
        while p < 4096:
            valid = struct.unpack_from('?', self.pool.buf, (addr << 12) + p + RecOff.valid)[0]
            if valid:
                valid_num += 1
            else:
                invalid_num += 1
            p += gap
        return valid_num, invalid_num

    def select_record(self, addr, cond_list, index_cond=None):
        """
        在一页中按照条件选择记录,
        :param index_cond: 索引使用的条件
        :param addr:页的地址
        :param cond_list:其他的条件
        :return:符合条件的记录列表 res
        """
        res = []
        p = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]
        fmt_size = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.fmt_size)[0]
        fmt = struct.unpack_from(str(fmt_size) + 's', self.pool.buf, (addr << 12) + Off.fmt)[0]
        while p != 0:
            r = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
            match = check_cond(r, cond_list)
            if match:
                res.append(r)
            ahead = go_ahead(r, index_cond)
            if not ahead:
                break
            p = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
        pass
        return res


def check_cond(r, cond_list):
    """
    对record进行检测
    :param r:
    :param cond_list:
    :return: True
    """
    for cond in cond_list:
        if cond[1] == "=":
            if r[cond[0]] != cond[2]:
                return False
        elif cond[1] == "<":
            if r[cond[0]] >= cond[2]:
                return False
        elif cond[1] == "<=":
            if r[cond[0]] > cond[2]:
                return False
        elif cond[1] == ">":
            if r[cond[0]] <= cond[2]:
                return False
        elif cond[1] == ">=":
            if r[cond[0]] < cond[2]:
                return False
        elif cond[1] == "<>":
            if r[cond[0]] == cond[2]:
                return False
    return True


def go_ahead(r, index_cond):
    """
    判断是否对下一条记录进行检查，在一页中总是从左往右检查的
    :param r:
    :param index_cond:
    :return:
    """
    if index_cond is None:
        return True
    if index_cond[1] == "=":
        if r[index_cond[0]] == index_cond[2]:
            return False
    elif index_cond[1] == "<":
        if r[index_cond[0]] >= index_cond[2]:
            return False
    elif index_cond[1] == "<=":
        if r[index_cond[0]] > index_cond[2]:
            return False

    return True
