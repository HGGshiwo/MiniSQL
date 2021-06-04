import struct
from CatalogManager import CatalogManager
from BufferManager import Off
from enum import IntEnum
import re

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

def true(record,table_name,condition):#判断record是否满足其中的table符合condition，并输出符合的cols，比如true(record,sname,student,age>18)就是搜索record里面的student表当中符合age>18的，输出sname列。
    nowtable=None
    if table_name in record:#如果这个record记录中存在这个table
        nowtable=record[table_name]
        #现在进入到record的table_name列，例如{"table1":{"name", "data_num", "index_num","fmt","column_list","index_list","primary_key"}"
        #table2":{"name", "data_num", "index_num","fmt","column_list","index_list","primary_key"}}为record，table1为table_name，
        # 那么现在nowtable为{"name", "data_num", "index_num","fmt","column_list","index_list","primary_key"}
    else:
        return False
        #否则，这个record里面不存在这个table，那么就找不到符合条件的值
    nowtable_columnlist=nowtable["column_list"]
    if condition is not None:
        exps=condition.split('and')#将不同条件分隔开
        if len(exps)==1:#如果只有一个条件
            exp=exps[0].strip()
            match=re.match(r'^([A-Za-z0-9_]+)\s*([<>=]+)\s*(.+)$', exp, re.S)

            if match and len(match.groups())==3:
                nowcol,op,value=match.groups()
                '''
                例如age>18,nowcol为"age"，op为">",value为"18"
                '''
                value=eval(value)#将"18"转换为18，字符串转换为数组
                for col in nowtable_columnlist:
                    if nowcol==col:#如果当前的列是要寻找的列
                        if(op=="="):
                            return nowtable_columnlist[nowcol]==value
                        elif op==">":
                            return nowtable_columnlist[nowcol]>value
                        elif op=="<":
                            return nowtable_columnlist[nowcol]<value
                        elif op==">=":
                            return nowtable_columnlist[nowcol]>=value
                        elif op=="<=":
                            return nowtable_columnlist[nowcol]<=value
                        elif op=="!=":
                            return nowtable_columnlist[nowcol]!=value
