import struct
from BufferManager import Off
from RecordManager import RecordManager, RecOff
from multiprocessing import shared_memory
from enum import IntEnum
import re


class TabOff(IntEnum):
    primary_key = 0
    leaf_header = 1
    name = 0
    fmt = 1
    unique = 2
    index_page = 3


class IndexManager(RecordManager):
    def __init__(self):
        RecordManager.__init__(self)

    def new_table(self, table_name, primary_key, table_info):
        """
        为新表格在catalog中注册
        :param table_name:
        :param primary_key:
        :param table_info: 顺序是name, fmt, unique, index_page全部是-1
        :return:
        """
        if self.catalog_list.count(-1) == 0:
            raise Exception('B1')

        if self.catalog_list.count(table_name) == 1:
            raise Exception('T1')

        # 在catalog_list中注册该表
        index = self.catalog_list.index(-1)
        self.catalog_list[index] = table_name

        # 为该表开辟文件空间存储
        fmt = ''
        for i in range(len(table_info)):
            if i % 4 == 1:
                fmt = fmt + table_info[i]
        page_no = self.new_root(True, fmt)

        # 将该表的信息写入内存
        table = [primary_key, page_no]
        table = table + table_info
        table[(primary_key << 2) + 5] = page_no  # 为primary key的属性index_page赋值
        self.table_list[table_name] = shared_memory.ShareableList(sequence=table, name=table_name)
        self.unpin_page(page_no)
        pass

    def drop_table(self, table_name):
        """
        删除record文件
        """
        pass

    def new_root(self, is_leaf, fmt):
        """
         创建新的根节点，不会在catalog中修改，因此需要外部修改catalog
        会直接写入buffer_pool中，可以从buffer_pool中获得该页
        :param is_leaf: 是否是叶子
        :param fmt: 新的根解码形式
        :return:新根所在的page_no
        """
        # 为tree增加一页
        addr, page_no = self.new_buffer()
        fmt_size = len(fmt)
        struct.pack_into('3i', self.pool.buf, (addr << 12), page_no, -1, 0)
        struct.pack_into('?', self.pool.buf, (addr << 12) + Off.is_leaf, is_leaf)
        struct.pack_into('3i', self.pool.buf, (addr << 12) + Off.previous_page, -1, -1, fmt_size)
        fmt = str.encode(fmt)
        struct.pack_into(str(fmt_size)+'s', self.pool.buf, (addr << 12) + Off.fmt, fmt)
        return page_no

    def insert_index(self, value_list, table_name, index):
        """
        从叶子插入，然后保持树
        :param value_list: 一条记录
        :param table_name: 当前操作的表
        :param index: 当前的索引是第几个
        :return: None
        """
        # self.pin_catalog(table_name)
        if self.catalog_list.count(table_name) == 0:
            raise Exception('T2')

        table = self.table_list[table_name]
        page_no = table[2 + (index << 2) + TabOff.index_page]  # 根节点
        index_fmt = str(table[2 + (index << 2) + TabOff.fmt]) + 'i'  # 索引的解码方式，是索引的个数+i。i表示地址
        if self.addr_list.count(page_no) == 0:
            self.load_page(page_no)
        addr = self.addr_list.index(page_no)
        is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]

        # 循环到叶子节点
        while not is_leaf:
            p = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]  # p是记录的指针, addr是页的指针
            last_page = None  # 上一条记录指向的页
            while True:
                if p == 0:
                    page_no = last_page
                    break
                r = struct.unpack_from(index_fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
                if r[1] > value_list[index]:
                    page_no = last_page
                    break
                pass
                last_page = r[0]
                p = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
            pass
            # 如果不存在比它大的记录，则进入到最后一页
            if self.addr_list.count(page_no) == 0:
                self.load_page(page_no)
            addr = self.addr_list.index(page_no)
            is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]
        pass

        # 迭代变量初始化， 全部是被插入页的属性
        cur_fmt_size = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.fmt_size)[0]
        cur_fmt = struct.unpack_from(str(cur_fmt_size) + 's', self.pool.buf, (addr << 12) + Off.fmt)[0]
        cur_page = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.current_page)[0]
        cur_addr = addr  # 被插入的页
        cur_index = index  # 被插入的页对应的索引位置
        cur_value_list = value_list  # 被插入的值
        # 开始迭代
        while True:
            # 首先将这条记录插入
            head_value, new_record = self.insert_record(cur_addr, cur_value_list, cur_index)
            if head_value is not None:
                self.replace_value(cur_page, index_fmt, head_value)
            if new_record is None:
                break

            # 如果原来的页已满，则分裂出新页，将new_record插入新页
            # right是新的，而且数据大
            right_addr, right_page = self.new_buffer()
            # 首先对新页的页首赋值
            self.pool.buf[(right_addr << 12) + Off.is_leaf: (right_addr << 12) + Off.fmt + cur_fmt_size] \
                = self.pool.buf[(cur_addr << 12) + Off.is_leaf:(cur_addr << 12) + Off.fmt + cur_fmt_size]
            struct.pack_into('i', self.pool.buf, (right_addr << 12) + Off.header, 0)
            struct.pack_into('i', self.pool.buf, (right_addr << 12) + Off.current_page, right_page)

            # 然后维护页链表
            next_page = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.next_page)[0]
            struct.pack_into('i', self.pool.buf, (right_addr << 12) + Off.next_page, next_page)
            struct.pack_into('i', self.pool.buf, (cur_addr << 12) + Off.next_page, right_page)
            if next_page != -1:
                if self.addr_list.count(next_page) == 0:
                    self.load_page(next_page)
                next_addr = self.addr_list.index(next_page)
                struct.pack_into('i', self.pool.buf, (next_addr << 12) + Off.previous_page, right_page)

            valid_num, invalid_num = self.count_valid(right_addr)  # 记录的数目
            n = valid_num + invalid_num
            # 转移一半的记录
            p = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.header)[0]
            # 计算出旧页的第一条值，这也是旧页在父节点的索引
            # 找到前一半数据，在旧页不动， p是旧页理论上最后一条数据
            for i in range((n + 1) // 2):
                p = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + p + RecOff.next_addr)[0]

            # 记录下p下一条数据q，q作为新页的第一条数据，将pnext指向0
            q = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + p + RecOff.next_addr)[0]
            struct.pack_into('i', self.pool.buf, (cur_addr << 12) + p + RecOff.next_addr, 0)

            # 在新页插入后一半值
            while q != 0:
                r = struct.unpack_from(cur_fmt, self.pool.buf, (cur_addr << 12) + q + RecOff.record)
                self.insert_record(right_addr, r, cur_index)  # 插入到addr中
                struct.pack_into('?', self.pool.buf, (cur_addr << 12) + q + RecOff.valid, False)  # 从addr中删除
                q = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + q + RecOff.next_addr)[0]

            # 把不成功的这条插入到新页中, 一定不会溢出
            head_value, t = self.insert_record(right_addr, new_record, cur_index)

            # 如果新页是非叶节点，把新页的孩子指向新页
            is_leaf = struct.unpack_from('?', self.pool.buf, (right_addr << 12) + Off.is_leaf)[0]
            if not is_leaf:
                p = struct.unpack_from('i', self.pool.buf, (right_addr << 12) + Off.header)[0]
                while p != 0:
                    child_page = struct.unpack_from('i', self.pool.buf, (right_addr << 12) + p + RecOff.record)[0]
                    if self.addr_list.count(child_page) == 0:
                        self.load_page(child_page)
                    child_addr = self.addr_list.index(child_page)
                    struct.pack_into('i', self.pool.buf, (child_addr << 12) + Off.parent, right_addr)
                    p = struct.unpack_from('i', self.pool.buf, (child_addr << 12) + p + RecOff.next_addr)[0]

            parent = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.parent)[0]
            if parent == -1:
                # 如果是根的分裂，创建一个新根
                parent = self.new_root(False, index_fmt)
                self.table_list[table_name][5 + (index << 2)] = parent
                struct.pack_into('i', self.pool.buf, (right_addr << 12) + Off.parent, parent)
                struct.pack_into('i', self.pool.buf, (cur_addr << 12) + Off.parent, parent)

                # 把两个孩子插入到新根中，不打算迭代了，因此不叫cur_value_list
                parent_addr = self.addr_list.index(parent)
                p = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.header)[0]
                r = struct.unpack_from(cur_fmt, self.pool.buf, (cur_addr << 12) + p + RecOff.record)
                value_list = [cur_addr, r[cur_index]]
                self.insert_record(parent_addr, value_list, 1)

                p = struct.unpack_from('i', self.pool.buf, (right_addr << 12) + Off.header)[0]
                r = struct.unpack_from(cur_fmt, self.pool.buf, (right_addr << 12) + p + RecOff.record)
                value_list = [right_addr, r[cur_index]]
                self.insert_record(parent_addr, value_list, 1)
                break

            # 如果父节点存在，则把新页需要插入的传递到下一个循环
            p = struct.unpack_from('i', self.pool.buf, (right_addr << 12) + Off.header)[0]
            r = struct.unpack_from(cur_fmt, self.pool.buf, (right_addr << 12) + p + RecOff.record)
            cur_value_list = [right_addr, r[cur_index]]  # 想要插入到父页的数据
            if self.addr_list.count(parent) == 0:
                self.load_page(parent)
            cur_addr = self.addr_list.index(parent)
            cur_fmt_size = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.fmt_size)[0]
            cur_fmt = struct.unpack_from(str(cur_fmt_size) + 's', self.pool.buf, (cur_addr << 12) + Off.fmt)
            cur_index = 1

    def delete_index(self, table_name, index, value):
        """
        当我找到一条记录后，主索引值，其他索引值都知道了，因此可以执行删除索引值是value的记录

        :param table_name:
        :param index:
        :param value:
        :return:
        """
        self.pin_catalog(table_name)
        if table_name not in self.table_list.keys():
            raise Exception('T2')
        table = self.table_list[table_name]
        page_no = table[2 + (index >> 2) + TabOff.index_page]  # 根节点
        index_fmt = str(table[2 + (index >> 2) + TabOff.fmt]) + 'i'  # 索引的解码方式，是索引的个数+i。i表示地址
        if self.addr_list.count(page_no) == 0:
            self.load_page(page_no)
        addr = self.addr_list.index(page_no)
        is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]

        # 循环到叶子节点
        while not is_leaf:
            p = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]  # p是记录的指针, addr是页的指针
            last_page = None  # 上一条记录指向的页

            while True:
                if p == 0:
                    # 如果不存在比它大的记录，则进入到最后一页
                    page_no = last_page
                    break
                r = struct.unpack_from(index_fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
                if r[1] > value:
                    page_no = last_page
                    break

                last_page = r[0]
                p = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]

            if self.addr_list.count(page_no) == 0:
                self.load_page(page_no)
            addr = self.addr_list.index(page_no)
            is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]

        half_empty, head_value = self.delete_record(addr, index, value)  # 在addr中删除一条记录
        parent_page = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.parent)[0]
        fmt_size = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.fmt_size)[0]
        fmt = struct.unpack_from(str(fmt_size) + 's', self.pool.buf, (addr << 12) + Off.fmt)[0]

        # 如果需要进行操作
        cur_fmt = fmt  # 当前页解码方式（父页一定是index_fmt）
        cur_index = index  # 当前页索引，父页一定是1
        cur_page = page_no  # 当前页号
        cur_addr = addr  # 当前页地址
        cur_parent = parent_page
        #  开始迭代
        while half_empty and cur_parent != -1:
            merge_left = None
            # 如果修改了第一条记录，那么循环上去修改
            if head_value is not None:
                self.replace_value(cur_page, index_fmt, head_value)
            if self.addr_list.count(cur_parent) == 0:
                self.load_page(cur_parent)
            cur_parent_addr = self.addr_list.index(cur_parent)

            # 找到该条记录的旁边一条记录
            p = struct.unpack_from('i', self.pool.buf, (cur_parent_addr << 12) + Off.header)[0]
            record = None
            while p != 0:
                r = struct.unpack_from(index_fmt, self.pool.buf, (cur_parent_addr << 12) + p + RecOff.record)
                pre_addr = struct.unpack_from('i', self.pool.buf, (cur_parent_addr << 12) + p + RecOff.pre_addr)[0]
                next_addr = struct.unpack_from('i', self.pool.buf, (cur_parent_addr << 12) + p + RecOff.next_addr)[0]
                if r[0] == cur_page:
                    if pre_addr != 0:
                        merge_left = True  # 默认merge_left
                        record = struct.unpack_from(
                            index_fmt, self.pool.buf, (cur_parent_addr << 12) + pre_addr + RecOff.record)
                    else:
                        merge_left = False
                        record = struct.unpack_from(
                            index_fmt, self.pool.buf, (cur_parent_addr << 12) + next_addr + RecOff.record)
                    break
                p = struct.unpack_from('i', self.pool.buf, (cur_parent_addr << 2) + p + RecOff.next_addr)[0]

            # 计算帮助其合并的页的页号
            merge_page = record[0]
            if self.addr_list.count(merge_page) == 0:
                self.load_page(merge_page)
            merge_addr = self.addr_list.index(merge_page)
            valid_num, invalid_num = self.count_valid(merge_addr)

            # 如果半满， 发生合并
            if valid_num == invalid_num:

                # 将被合并页所有数据转移到merge_page中
                p = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.header)[0]
                while p != 0:
                    r = struct.unpack_from(cur_fmt, self.pool.buf, (cur_addr << 12) + p + RecOff.record)
                    self.insert_record(merge_addr, r, cur_index)
                    p = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + p + RecOff.next_addr)

                # 如果是叶子，需要将其从文件链表中删除
                is_leaf = struct.unpack_from('?', self.pool.buf, (cur_addr << 12) + Off.is_leaf)[0]
                if is_leaf:
                    pre_page = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.previous_page)
                    next_page = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.next_page)
                    if pre_page == -1:
                        table[TabOff.leaf_header] = next_page
                    else:
                        if self.addr_list.count(pre_page) == 0:
                            self.load_page(pre_page)
                        pre_addr = self.addr_list.index(pre_page)
                        struct.pack_into('i', self.pool.buf, (pre_addr << 12) + Off.next_page, next_page)
                    if next_page != -1:
                        if self.addr_list.count(next_page) == 0:
                            self.load_page(next_page)
                        next_addr = self.addr_list.index(next_page)
                        struct.pack_into('i', self.pool.buf, (next_addr << 12) + Off.previous_page, pre_page)

                # 在缓存和文件物理列表中删除这个文件
                self.delete_buffer(cur_page)

                # 更新迭代变量，到父节点中去删除
                half_empty, head_value = self.delete_record(cur_parent_addr, 0, cur_page)
                cur_fmt = index_fmt
                cur_index = 1
                cur_page = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.parent)[0]
                if self.addr_list.count(cur_page) == 0:
                    self.load_page(cur_page)
                cur_addr = self.addr_list.index(cur_page)

            # 如果大于一半，转移一条记录
            else:
                if merge_left:
                    # 如果左边页转移数据，那么删除最后一条数据，将其加入到被merge页
                    p = struct.unpack_from('i', self.pool.buf, (merge_addr << 12) + Off.header)[0]
                    q = p  # q是p的前一条，需要找最后一条记录p
                    while True:
                        next_addr = struct.unpack_from('i', self.pool.buf, (merge_addr << 12) + p + RecOff.next_addr)
                        if next_addr == 0:
                            # 找到了最后一条记录p
                            struct.pack_into('i', self.pool.buf, (merge_addr << 12) + q + RecOff.next_addr, 0)
                            struct.pack_into('?', self.pool.buf, (merge_addr << 12) + p + RecOff.valid, False)
                            r = struct.unpack_from(cur_fmt, self.pool.buf, (merge_addr << 12) + p + RecOff.record)
                            self.insert_record(cur_addr, r, cur_index)
                            # 循环上去修改索引
                            self.replace_value(cur_page, index_fmt, head_value)
                            break

                        q = p
                        p = struct.unpack_from('i', self.pool.buf, (merge_addr << 12) + p + RecOff.next_addr)[0]
                else:
                    # 如果右边转移数据，那么删除第一条数据，将其插入merge页
                    p = struct.unpack_from('i', self.pool.buf, (merge_addr << 12) + Off.header)[0]
                    next_addr = struct.unpack_from('i', self.pool.buf, (merge_addr << 12) + p + RecOff.next_addr)[0]
                    r = struct.unpack_from(cur_fmt, self.pool.buf, (merge_addr << 12) + p + RecOff.record)
                    struct.pack_into('i', self.pool.buf, (merge_addr << 12) + Off.header, next_addr)
                    self.insert_record(cur_addr, r, cur_index)

                # 退出外循环
                break

    def replace_value(self, page_no, index_fmt, head_value):
        """
        在page_no的父节点，把page_no对应的value替换为head_value，一直到根
        :param page_no:
        :param index_fmt:
        :param head_value: 想要修改成的值
        :return:
        """
        is_delete = True
        if self.addr_list.count(page_no) == 0:
            self.load_page(page_no)
        addr = self.addr_list.index(page_no)
        parent = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.parent)[0]

        while is_delete and parent != -1:
            if self.addr_list.count(parent) == 0:
                self.load_page(parent)
            parent_addr = self.addr_list.index(parent)

            p = struct.unpack_from('i', self.pool.buf, (parent_addr << 12) + Off.header)[0]
            # 找到这条记录，修改它
            while True:
                r = list(struct.unpack_from(
                    index_fmt, self.pool.buf, (parent_addr << 12) + p + RecOff.record))
                pre_addr = struct.unpack_from(
                    'i', self.pool.buf, (parent_addr << 12) + p + RecOff.pre_addr)[0]
                if r[0] == page_no:
                    r[1] = head_value
                    struct.pack_into(
                        index_fmt, self.pool.buf, (parent_addr << 12) + p + RecOff.record, *r)
                    is_delete = True if pre_addr == 0 else False
                    break
                p = struct.unpack_from(
                    'i', self.pool.buf, (parent_addr << 12) + p + RecOff.next_addr)[0]
            page_no = parent
            parent = struct.unpack_from('i', self.pool.buf, (parent_addr << 12) + Off.parent)[0]

    def select_page(self, table_name, cond_list=None):
        """
        返回符合条件的值，是一个列表的列表
        :param table_name:
        :param cond_list:列表,放不同的语句，类似于['age > 1','s == 0']
        :return:
        """
        if table_name not in self.table_list.keys():
            raise Exception('T2')
        table = self.table_list[table_name]
        primary_key = table[TabOff.primary_key]
        primary_page = table[(primary_key << 2) + 5]  # 主索引所在根
        # 命令预处理
        index_cond = None
        primary_cond = None
        page_no = -1  # 索引的根
        index_fmt = None
        for i, cond in enumerate(cond_list):
            cond = re.match(r'^([A-Za-z0-9_]+)\s*([<>=]+)\s*(.+)$', cond, re.S)
            column_name, op, value = cond.groups()

            # 将name转为数字
            column = (table.index(column_name) - 2) // 4
            # 将value转为数字
            if value.isdigit():
                value = eval(value)
            else:
                value = value.encode()
            cond_list[i] = [column, op, value]

            index_page = table.index(column_name) + 3
            fmt = table.index(column_name) + 1
            if cond_list[i][1] != '<>':
                if table[index_page] != -1:
                    if column == primary_key:
                        primary_cond = cond_list[i]
                        page_no = table[index_page]
                        index_fmt = 'i' + table[fmt]
                    elif index_cond is None:
                        index_cond = cond_list[i]
                        cond_list.pop(cond_list[i])
                        page_no = table[index_page]
        pass

        res = []
        # 如果是主索引查询，首先到根， 不等于也是遍历全部
        if primary_cond is not None:
            if self.addr_list.count(page_no) == 0:
                self.load_page(page_no)
            addr = self.addr_list.index(page_no)
            is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]
            while not is_leaf:
                p = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]
                last_page = None
                while True:
                    if p == 0:
                        page_no = last_page
                        break
                    r = struct.unpack_from(index_fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
                    if r[1] > primary_cond[2]:
                        page_no = last_page
                        break
                    last_page = r[0]
                    p = struct.unpack_from("i", self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
                pass
                if self.addr_list.count(page_no) == 0:
                    self.load_page(page_no)
                addr = self.addr_list.index(page_no)
                is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]

            # 如果到了页节点，根据操作符判断是否到下一个页
            while page_no != -1:
                if self.addr_list.count(page_no) == 0:
                    self.load_page(page_no)
                addr = self.addr_list.index(page_no)
                page_res = self.select_record(addr, cond_list, primary_cond)
                res.extend(page_res)
                if primary_cond[1] == "=":
                    break
                elif primary_cond[1] == ">" or primary_cond[1] == ">=":
                    page_no = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.next_page)
                elif primary_cond[1] == "<" or primary_cond[1] == "<=":
                    page_no = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.previous_page)
                else:
                    raise Exception('E1')

        # 如果是二级索引查询
        elif index_cond is not None:
            primary_value = []
            if self.addr_list.count(page_no) == 0:
                self.load_page(page_no)
            addr = self.addr_list.index(page_no)
            is_leaf = struct.unpack_from('?', (addr << 12) + Off.is_leaf)[0]
            while not is_leaf:
                p = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]
                last_page = None
                while True:
                    if p == 0:
                        page_no = last_page
                        break
                    r = struct.unpack_from(index_fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
                    if r[1] > index_cond[2]:
                        page_no = last_page
                        break
                    last_page = r[0]
                    p = struct.unpack_from("i", self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
                pass
                if self.addr_list.count(page_no) == 0:
                    self.load_page(page_no)
                addr = self.addr_list.index(page_no)
                is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]

            # 如果到了页节点，根据操作符判断是否到下一个页
            while page_no != -1:
                if self.addr_list.count(page_no) == 0:
                    self.load_page(page_no)
                addr = self.addr_list.index(page_no)
                value = self.select_record(addr, cond_list, index_cond)
                primary_value.extend(value)
                if index_cond[1] == "=":
                    break
                elif index_cond[1] == ">" or index_cond[1] == ">=":
                    page_no = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.next_page)
                elif index_cond[1] == "<" or index_cond[1] == "<=":
                    page_no = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.previous_page)
                else:
                    raise Exception('E1')

            # 回表
            for value in primary_value:
                if self.addr_list.count(primary_page) == 0:
                    self.load_page(primary_page)
                addr = self.addr_list.index(primary_page)
                is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]
                # 循环到叶子节点
                while not is_leaf:
                    p = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]
                    last_page = None
                    while True:
                        if p == 0:
                            page_no = last_page
                            break
                        r = struct.unpack_from(index_fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
                        if r[1] > value:
                            page_no = last_page
                            break
                        pass
                        last_page = r[0]
                        p = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
                    pass
                    if self.addr_list.count(page_no) == 0:
                        self.load_page(page_no)
                    addr = self.addr_list.index(page_no)
                    is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]
                pass

                # 在叶子中查找
                p = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]
                fmt_size = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.fmt_size)[0]
                fmt = struct.unpack_from(str(fmt_size) + 's', self.pool.buf, (addr << 12) + Off.fmt)[0]
                while p != 0:
                    r = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
                    if r[primary_key] == value:
                        res.append(r)
                        break
                    p = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)
                pass
            pass

        # 顺序查询
        else:
            leaf_header = table[TabOff.leaf_header]
            page = leaf_header
            while page != -1:
                if self.addr_list.count(page) == 0:
                    self.load_page(page)
                addr = self.addr_list.index(page)
                page_res = self.select_record(addr, cond_list)
                res.extend(page_res)
                page = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.next_page)[0]
            pass
        return res