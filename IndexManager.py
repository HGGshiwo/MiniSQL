import struct
from BufferManager import Off
from RecordManager import RecordManager, RecOff
from multiprocessing import shared_memory
from enum import IntEnum


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
        # 在catalog_list中注册该表
        if self.catalog_list.count(-1) == 0:
            print("以达到建表上界，如果想新建表，请先删除一个表")
            return
        index = self.catalog_list.index(-1)
        self.catalog_list[index] = table_name

        # 为该表开辟文件空间存储
        fmt = ''
        for i in range(len(table_info)):
            if i % 4 == 1:
                fmt = fmt + table_info[i]
        index_no = primary_key//4
        page_no = self.new_root(True, fmt, index_no)

        # 将该表的信息写入内存
        table = [primary_key, page_no]
        table_info[primary_key + 3] = page_no
        table = table + table_info
        self.table_list[table_name] = shared_memory.ShareableList(sequence=table, name=table_name)
        self.unpin_page(page_no)
        pass

    def delete_table(self, table_name):
        """
        删除record文件
        """
        pass

    def new_root(self, is_leaf, fmt, index_offset):
        """
         创建新的根节点，不会在catalog中修改，因此需要外部修改catalog
        会直接写入buffer_pool中，可以从buffer_pool中获得该页
        :param is_leaf: 是否是叶子
        :param fmt: 新的根解码形式
        :param index_offset: 新的根索引在value_list的位置
        :return:新根所在的page_no
        """
        # 为tree增加一页
        addr, page_no = self.new_buffer()
        fmt_size = len(fmt)
        struct.pack_into('4i', self.pool.buf, (addr << 12), page_no, -1, 0, 0)
        struct.pack_into('?', self.pool.buf, (addr << 12) + Off.is_leaf, is_leaf)
        struct.pack_into('4i', self.pool.buf, (addr << 12) + Off.previous_page, -1, -1, index_offset, fmt_size)
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
        self.pin_catalog(table_name)
        table = self.table_list[table_name]
        page_no = table[2 + (index >> 2) + TabOff.index_page]  # 根节点
        index_fmt = str(table[2 + (index >> 2) + TabOff.fmt]) + 'i'  # 索引的解码方式，是索引的个数+i。i表示地址
        addr = self.addr_list.index(page_no)
        is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]

        # 循环到叶子节点
        r = [0, 0]
        while not is_leaf:
            p = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]  # p是记录的指针, addr是页的指针
            while p != 0:
                r = struct.unpack_from(index_fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
                if r[index] > value_list[index]:
                    offset = (addr << 12) + p + struct.calcsize(index_fmt)
                    addr = struct.unpack_from('i', self.pool.buf, offset)[0]
                    break
                p = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]

            if p == 0:
                addr = r[0]
            is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]
        ret = addr  # 返回的是叶子的地址
        fmt_size = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.fmt_size)[0]
        fmt = struct.unpack_from(str(fmt_size) + 's', self.pool.buf, (addr << 12) + Off.fmt)[0]
        page_no = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.current_page)[0]

        # 用主键插入
        # page是当前操作的页，下一个循环前更新为下一个想分裂的页，这个最后写入
        # left_page, right_page是page分裂后的两页，下一个循环前写入
        is_full = self.insert_record(addr, value_list)
        right_addr = addr
        while is_full:
            # 新建一页
            left_addr, left_page_no = self.new_buffer()  # left是新的

            # 首先对新页的页首赋值
            self.pool.buf[(left_addr << 12) + Off.is_leaf: (left_addr << 12) + Off.fmt + fmt_size] \
                = self.pool.buf[(right_addr << 12) + Off.is_leaf:(right_addr << 12) + Off.fmt + fmt_size]
            struct.pack_into('i', self.pool.buf, (left_addr << 12) + Off.current_page, left_page_no)
            struct.pack_into('i', self.pool.buf, (left_addr << 12) + Off.next_page, page_no)
            struct.pack_into('i', self.pool.buf, (left_addr << 12) + Off.header, Off.fmt + fmt_size)
            n = 4063//(13 + struct.calcsize(fmt))  # 记录的数目

            # 把记录插入进去
            p = struct.unpack_from('i', self.pool.buf, (right_addr << 12) + Off.header)[0]
            for i in range(n//2):
                r = struct.unpack_from(fmt, self.pool.buf, (right_addr << 12) + p + RecOff.record)
                self.insert_record(left_addr, r)  # 插入到addr中
                struct.pack_into('?', self.pool.buf, (right_addr << 12) + p + RecOff.valid, False)  # 从addr中删除
                p = struct.unpack_from('i', self.pool.buf, (right_addr << 12) + p + RecOff.next_addr)[0]

            pre_page_no = struct.unpack_from('i', self.pool.buf, (right_addr << 12) + Off.previous_page)[0]
            # 修改旧页的数据,使得p成为right第一条数据
            struct.pack_into('i', self.pool.buf, (right_addr << 12) + Off.header, p)
            struct.pack_into('i', self.pool.buf, (right_addr << 12) + p + RecOff.pre_addr, 0)
            struct.pack_into('i', self.pool.buf, (right_addr << 12) + Off.previous_page, left_addr)
            self.insert_record(right_addr, value_list)  # 把不成功的这条插入到旧页中
            # 如果新页是非叶节点，把新页的孩子指向新页
            is_leaf = struct.unpack_from('?', self.pool.buf, (right_addr << 12) + Off.is_leaf)[0]
            if not is_leaf:
                p = struct.unpack_from('i', self.pool.buf, (left_addr << 12) + Off.header)[0]
                while p != 0:
                    child_addr = struct.unpack_from('i', self.pool.buf, (left_addr << 12) + p + RecOff.record)[0]
                    if self.addr_list.count(child_addr) == 0:
                        self.load_page(child_addr)
                    child_addr = self.addr_list.index(child_addr)
                    child_addr = child_addr << 12
                    struct.pack_into('i', self.pool.buf, child_addr + Off.parent, left_addr)
                    p = struct.unpack_from('i', self.pool.buf, child_addr + p + RecOff.next_addr)[0]
            else:
                # 如果是叶节点的分裂，则修改旧页前一页的数据。让它指向新页
                if pre_page_no != -1:
                    if self.addr_list.count(pre_page_no) == 0:  # 首先确保page_no在addr_list中
                        self.load_page(pre_page_no)
                    pre_addr = self.addr_list.index(pre_page_no)
                    struct.pack_into('i', self.pool.buf, (pre_addr << 12) + Off.next_page, left_page_no)
                else:
                    table[TabOff.leaf_header] = left_page_no
            # 如果是根的分裂，创建一个新根
            parent = struct.unpack_from('i', self.pool.buf, (right_addr << 12)+Off.parent)[0]
            if parent == -1:
                page_no = self.new_root(False, index_fmt, index)
                table[5 + (index << 2)] = page_no
                struct.pack_into('i', self.pool.buf, (left_addr << 12) + Off.parent, page_no)
                struct.pack_into('i', self.pool.buf, (right_addr << 12) + Off.parent, page_no)

                # 把两个孩子插入到新根中
                addr = self.addr_list.index(page_no)
                r = struct.unpack_from(fmt, self.pool.buf, (right_addr << 12) + Off.header)
                value_list = [right_addr, r[index]]
                self.insert_record(addr, value_list)

                r = struct.unpack_from(fmt, self.pool.buf, (left_addr << 12) + Off.header)
                value_list = [left_addr, r[index]]
                self.insert_record(addr, value_list)
                is_full = False

            else:
                page_no = struct.unpack_from('i', self.pool.buf, (left_addr << 12) + Off.parent)[0]
                if self.addr_list.count(page_no) == 0:  # 首先确保page_no在addr_list中
                    self.load_page(page_no)
                addr = self.addr_list.index(page_no)
                r = struct.unpack_from(fmt, self.pool.buf, (right_addr << 12) + Off.header)
                value_list = [right_addr, r[index]]
                is_full = self.insert_record(addr, value_list)
        return ret  # 返回插入的叶子节点的index

    # def delete_tree(self, table_name, condition=None):
    #     """
    #     删除所搜寻的索引
    #     """
    #     self.pin_catalog(table_name)
    #     table = shared_memory.ShareableList(name=table_name)
    #     page_no = table[1]
    #     page = read_buffer(share, page_no)
    #     while not page[Page.isleaf]:#如果当前的page不是叶节点，那么循环到叶节点
    #         #page=page.child，即循环到孩子节点
    #         #i = 0
    #         #while i < len(page.user_record) - 1:
    #         #    i += 1
    #         page_no = page[Page.user_record][0][1]#到最左边的一页
    #         unpin_page(share, page_no)
    #         page = read_buffer(share, page_no)
    #     #现在循环到了最左侧的叶子节点，现在在叶子节点当中查询。
    #     while(page[Page.next_page]):#不是最右边的那一页
    #         user_record=page[Page.user_record]
    #         for item in user_record:
    #             if true(item,condition):#如果item符合condition
    #                 item=None
    #     self.unpin_catalog(table_name)

    # def select_page(self, table_name, condition=None):
    #     """
    #     查找page**********************************
    #     从根节点开始，如果符合条件那么返回来符合条件的索引；如果不符合条件那么继续进行
    #     """
    #     table_info = read_catalog(share, table_name)
    #     page_no=table_info[Table.page_header]
    #     page=read_buffer(share, page_no)
    #     while not page[Page.isleaf]:#如果当前的page不是叶节点，那么循环到叶节点
    #         #page=page.child，即循环到孩子节点
    #         #i = 0
    #         #while i < len(page.user_record) - 1:
    #         #    i += 1
    #         unpin_page(share, page_no)
    #         page_no = page[Page.user_record][0][1]#到最左边的一页
    #         page = read_buffer(share, page_no)
    #     #现在循环到了最左侧的叶子节点，现在在叶子节点当中查询。
    #     indexlist=[]
    #     while page[Page.next_page] != -1:#不是最右边的那一页
    #         user_record=page[Page.user_record]
    #         for item in user_record:
    #             if true(item,condition):#如果item符合condition
    #                 indexlist.append(item)
    #     return indexlist
