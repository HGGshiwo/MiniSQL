import struct
from BufferManager import Off
from RecordManager import RecordManager, RecOff


class IndexManager(RecordManager):
    def __init__(self):
        RecordManager.__init__(self)

    def print_page(self, page_no):
        addr = self.get_addr(page_no)
        print('------------------page ' + str(page_no) + ' info------------------')
        current_page = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.current_page)[0]
        print("current_page\t" + str(current_page))
        next_page = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.next_page)[0]
        print("next_page\t\t" + str(next_page))
        header = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]
        print("header\t\t\t" + str(header))
        is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]
        print("is_leaf\t\t\t" + str(is_leaf))
        parent = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.parent)[0]
        print("parent\t\t\t" + str(parent))
        previous_page = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.previous_page)[0]
        print("previous_page\t" + str(previous_page))
        fmt_size = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.fmt_size)[0]
        print("fmt_size\t\t" + str(fmt_size))
        fmt = struct.unpack_from(str(fmt_size) + 's', self.pool.buf, (addr << 12) + Off.fmt)[0]
        print('fmt\t\t\t\t' + str(fmt, encoding='utf8'))

        header = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]
        fmt_size = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.fmt_size)[0]
        fmt = struct.unpack_from(str(fmt_size) + 's', self.pool.buf, (addr << 12) + Off.fmt)[0]
        p = header
        while p != 0:
            pre = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.pre_addr)[0]
            r = struct.unpack_from(fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
            cur = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.curr_addr)[0]
            next_addr = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
            valid = struct.unpack_from('?', self.pool.buf, (addr << 12) + p + RecOff.valid)[0]
            print("valid:" + str(valid) + "\tcur:" + str(cur)
                  + "\tpre:" + str(pre) + '\tnext:' + str(next_addr) + '\tr:', end='')
            print(r)
            p = next_addr
        print('')

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

    def insert_index(self, value_list, page_no, index, index_fmt):
        """
        从叶子插入，然后保持树
        :param page_no: 根节点所在的页号
        :param index_fmt: 索引格式
        :param value_list: 一条记录
        :param index: 当前的索引是第几个
        :return: 新的根，如果被修改的话，否则是-1
        """
        addr = self.get_addr(page_no)
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
                if r[1] > value_list[index]:
                    page_no = last_page
                    # 如果不存在比它小的记录，则进入这一页
                    if page_no is None:
                        page_no = r[0]
                    break
                pass
                last_page = r[0]
                p = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
            pass

            addr = self.get_addr(page_no)
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
            struct.pack_into('i', self.pool.buf, (right_addr << 12) + Off.previous_page, cur_page)
            # 然后维护页链表
            next_page = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.next_page)[0]
            struct.pack_into('i', self.pool.buf, (right_addr << 12) + Off.next_page, next_page)
            struct.pack_into('i', self.pool.buf, (cur_addr << 12) + Off.next_page, right_page)

            if next_page != -1:
                next_addr = self.get_addr(next_page)
                struct.pack_into('i', self.pool.buf, (next_addr << 12) + Off.previous_page, right_page)

            valid_num, invalid_num = self.count_valid(right_addr)  # 记录的数目
            n = valid_num + invalid_num
            # 转移一半的记录
            p = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.header)[0]
            # 计算出旧页的第一条值，这也是旧页在父节点的索引
            # 找到前一半数据，在旧页不动， p是旧页理论上最后一条数据
            for i in range((n + 1) // 2):
                p = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + p + RecOff.next_addr)[0]

            # 记录下p下一条数据q，q作为新页的第一条数据，将p next指向0
            q = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + p + RecOff.next_addr)[0]
            struct.pack_into('i', self.pool.buf, (cur_addr << 12) + p + RecOff.next_addr, 0)

            # 在新页插入后一半值
            while q != 0:
                r = struct.unpack_from(cur_fmt, self.pool.buf, (cur_addr << 12) + q + RecOff.record)
                self.insert_record(right_addr, r, cur_index)  # 插入到addr中
                struct.pack_into('?', self.pool.buf, (cur_addr << 12) + q + RecOff.valid, False)  # 从addr中删除
                q = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + q + RecOff.next_addr)[0]

            # 把被替换结果插入到新页中, 一定不会溢出，也不修改头
            self.insert_record(right_addr, new_record, cur_index)

            # 如果新页是非叶节点，把新页的孩子指向新页
            is_leaf = struct.unpack_from('?', self.pool.buf, (right_addr << 12) + Off.is_leaf)[0]
            if not is_leaf:
                p = struct.unpack_from('i', self.pool.buf, (right_addr << 12) + Off.header)[0]
                while p != 0:
                    child_page = struct.unpack_from('i', self.pool.buf, (right_addr << 12) + p + RecOff.record)[0]
                    child_addr = self.get_addr(child_page)
                    struct.pack_into('i', self.pool.buf, (child_addr << 12) + Off.parent, right_addr)
                    p = struct.unpack_from('i', self.pool.buf, (child_addr << 12) + p + RecOff.next_addr)[0]

            parent = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.parent)[0]
            if parent == -1:
                # 如果是根的分裂，创建一个新根
                parent = self.new_root(False, index_fmt)
                struct.pack_into('i', self.pool.buf, (right_addr << 12) + Off.parent, parent)
                struct.pack_into('i', self.pool.buf, (cur_addr << 12) + Off.parent, parent)

                # 把两个孩子插入到新根中，不打算迭代了，因此不叫cur_value_list
                parent_addr = self.addr_list.index(parent)
                p = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.header)[0]
                r = struct.unpack_from(cur_fmt, self.pool.buf, (cur_addr << 12) + p + RecOff.record)
                value_list = [cur_page, r[cur_index]]
                self.insert_record(parent_addr, value_list, 1)

                p = struct.unpack_from('i', self.pool.buf, (right_addr << 12) + Off.header)[0]
                r = struct.unpack_from(cur_fmt, self.pool.buf, (right_addr << 12) + p + RecOff.record)
                value_list = [right_page, r[cur_index]]
                self.insert_record(parent_addr, value_list, 1)
                return parent

            # 如果父节点存在，则把新页需要插入的传递到下一个循环
            p = struct.unpack_from('i', self.pool.buf, (right_addr << 12) + Off.header)[0]
            r = struct.unpack_from(cur_fmt, self.pool.buf, (right_addr << 12) + p + RecOff.record)
            cur_value_list = [right_page, r[cur_index]]  # 想要插入到父页的数据
            cur_addr = self.get_addr(parent)
            cur_fmt_size = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.fmt_size)[0]
            cur_fmt = struct.unpack_from(str(cur_fmt_size) + 's', self.pool.buf, (cur_addr << 12) + Off.fmt)
            cur_index = 1
        pass
        return -1

    def delete_index(self, page_no, index, index_fmt, value):
        """
        当我找到一条记录后，主索引值，其他索引值都知道了，因此可以执行删除索引值是value的记录

        :param page_no:
        :param index_fmt:
        :param index:
        :param value:
        :return: leaf header, index page
        """
        addr = self.get_addr(page_no)
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
                    if page_no is None:
                        page_no = r[0]
                    break
                last_page = r[0]
                p = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
            pass
            addr = self.get_addr(page_no)
            is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]
        pass

        leaf_header = None
        index_page = None

        fmt_size = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.fmt_size)[0]
        fmt = struct.unpack_from(str(fmt_size) + 's', self.pool.buf, (addr << 12) + Off.fmt)[0]

        cur_fmt = fmt  # 当前页解码方式（父页一定是index_fmt）
        cur_index = index  # 当前页索引，父页一定是1，插入排序时候用到
        cur_page = page_no  # 当前页号
        cur_addr = addr  # 当前页地址
        cur_value = value  # 删除位置的值
        cur_delete = index  # 定位删除的位置, 在叶子删除是看index的value，在非叶子看页号
        #  开始迭代, 删除一条记录，检查是否合并，检查是否需要删除
        while True:
            # 删除一条记录
            half_empty, head_value = self.delete_record(cur_addr, cur_delete, cur_value)  # 在addr中删除一条记录
            if head_value is not None:
                self.replace_value(cur_page, index_fmt, head_value)

            parent = struct.unpack_from(index_fmt, self.pool.buf, (cur_addr << 12) + Off.parent)[0]
            is_leaf = struct.unpack_from('?', self.pool.buf, (cur_addr << 12) + Off.is_leaf)[0]

            # 非根节点，只有一个孩子, 不是叶子，删除
            valid_num, invalid_num = self.count_valid(cur_addr)
            if parent == -1 and not is_leaf and valid_num == 1:
                p = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.header)[0]
                index_page = struct.unpack_from(index_fmt, self.pool.buf, (cur_addr << 12) + p + RecOff.record)[0]
                index_page_addr = self.get_addr(index_page)
                self.delete_buffer(cur_addr)
                # 让孩子变为根节点
                struct.pack_into('i', self.pool.buf, (index_page_addr << 12) + Off.parent, -1)
                break

            # 叶子且有效记录为空，说明表被删完了
            if is_leaf and valid_num == 0:
                leaf_header = -1
                break

            # 检查是否合并或者转移, 非叶只要是2个以上都是合法的
            if not half_empty or parent == -1:
                break

            parent_addr = self.get_addr(parent)

            # 找到该条记录的旁边一条记录
            merge_left = None
            p = struct.unpack_from('i', self.pool.buf, (parent_addr << 12) + Off.header)[0]
            record = None
            while p != 0:
                r = struct.unpack_from(index_fmt, self.pool.buf, (parent_addr << 12) + p + RecOff.record)
                pre_addr = struct.unpack_from('i', self.pool.buf, (parent_addr << 12) + p + RecOff.pre_addr)[0]
                next_addr = struct.unpack_from('i', self.pool.buf, (parent_addr << 12) + p + RecOff.next_addr)[0]
                if r[0] == cur_page:
                    if pre_addr != 0:
                        merge_left = True  # 默认merge_left
                        record = struct.unpack_from(
                            index_fmt, self.pool.buf, (parent_addr << 12) + pre_addr + RecOff.record)
                    else:
                        merge_left = False
                        record = struct.unpack_from(
                            index_fmt, self.pool.buf, (parent_addr << 12) + next_addr + RecOff.record)
                    break
                p = struct.unpack_from('i', self.pool.buf, (parent_addr << 12) + p + RecOff.next_addr)[0]

            # 计算帮助其合并的页的页号
            merge_page = record[0]
            merge_addr = self.get_addr(merge_page)
            valid_num, invalid_num = self.count_valid(merge_addr)

            # 如果大于一半，转移一条记录
            if valid_num > invalid_num:
                if merge_left:
                    # 如果左边页转移数据，那么删除最后一条数据，将其加入到被merge页
                    p = struct.unpack_from('i', self.pool.buf, (merge_addr << 12) + Off.header)[0]
                    q = p  # q是p的前一条，需要找最后一条记录p
                    while True:
                        next_addr = struct.unpack_from(
                            'i', self.pool.buf, (merge_addr << 12) + p + RecOff.next_addr)[0]
                        if next_addr == 0:
                            # 找到了最后一条记录p
                            struct.pack_into('i', self.pool.buf, (merge_addr << 12) + q + RecOff.next_addr, 0)
                            struct.pack_into('?', self.pool.buf, (merge_addr << 12) + p + RecOff.valid, False)
                            r = struct.unpack_from(cur_fmt, self.pool.buf, (merge_addr << 12) + p + RecOff.record)
                            head_value, t = self.insert_record(cur_addr, r, cur_index)
                            # 循环上去修改索引
                            self.replace_value(cur_page, index_fmt, head_value)
                            break
                        q = p
                        p = struct.unpack_from('i', self.pool.buf, (merge_addr << 12) + p + RecOff.next_addr)[0]
                    pass
                else:
                    # 如果右边转移数据，那么删除第一条数据，将其插入merge页
                    p = struct.unpack_from('i', self.pool.buf, (merge_addr << 12) + Off.header)[0]
                    next_addr = struct.unpack_from('i', self.pool.buf, (merge_addr << 12) + p + RecOff.next_addr)[0]
                    r = struct.unpack_from(cur_fmt, self.pool.buf, (merge_addr << 12) + p + RecOff.record)
                    struct.pack_into('i', self.pool.buf, (merge_addr << 12) + Off.header, next_addr)
                    struct.pack_into('i', self.pool.buf, (merge_addr << 12) + p + RecOff.valid, False)
                    self.insert_record(cur_addr, r, cur_index)
                break
            else:
                # 如果半满， 发生合并
                # 将被合并页所有数据转移到merge_page中
                p = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.header)[0]
                r = struct.unpack_from(cur_fmt, self.pool.buf, (cur_addr << 12) + p + RecOff.record)
                while p != 0:
                    r = struct.unpack_from(cur_fmt, self.pool.buf, (cur_addr << 12) + p + RecOff.record)
                    head_value, t = self.insert_record(merge_addr, r, cur_index)
                    if head_value is not None:
                        self.replace_value(merge_page, index_fmt, head_value)
                    p = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + p + RecOff.next_addr)[0]

                # 将其从文件链表中删除
                pre_page = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.previous_page)[0]
                next_page = struct.unpack_from('i', self.pool.buf, (cur_addr << 12) + Off.next_page)[0]
                if pre_page == -1:
                    leaf_header = next_page
                else:
                    pre_addr = self.get_addr(pre_page)
                    struct.pack_into('i', self.pool.buf, (pre_addr << 12) + Off.next_page, next_page)
                if next_page != -1:
                    next_addr = self.get_addr(next_page)
                    struct.pack_into('i', self.pool.buf, (next_addr << 12) + Off.previous_page, pre_page)

                # 在缓存和文件物理列表中删除这个文件
                self.delete_buffer(cur_page)

                # 继续迭代
                cur_value = cur_page  # 删除当前页
                cur_page = parent
                cur_addr = parent_addr
                cur_index = 1
                cur_delete = 0

        return leaf_header, index_page

    def replace_value(self, page_no, index_fmt, head_value):
        """
        在page_no的父节点，把page_no对应的value替换为head_value，一直到根
        :param page_no:
        :param index_fmt:
        :param head_value: 想要修改成的值
        :return:
        """
        is_delete = True
        addr = self.get_addr(page_no)
        parent = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.parent)[0]

        while is_delete and parent != -1:
            parent_addr = self.get_addr(parent)
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

    def select_page(self, page_no, index_fmt, index_cond, cond_list):
        """
        在指定根页的树中查找
        :param index_cond:
        :param index_fmt:
        :param page_no:
        :param cond_list:列表,放不同的语句，类似于[[0, '=', 2],[1, '>', 3]]
        :return:
        """
        res = []
        addr = self.get_addr(page_no)
        is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]
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
                    if page_no is None:
                        page_no = r[0]
                    break
                last_page = r[0]
                p = struct.unpack_from("i", self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]
            pass
            addr = self.get_addr(page_no)
            is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]

        # 如果到了叶节点，根据操作符判断是否到下一个页
        while page_no != -1:
            addr = self.get_addr(page_no)
            page_res = self.select_record(addr, cond_list, index_cond)
            res.extend(page_res)
            if index_cond[1] == "=":
                break
            elif index_cond[1] == ">" or index_cond[1] == ">=":
                page_no = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.next_page)[0]
            elif index_cond[1] == "<" or index_cond[1] == "<=":
                page_no = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.previous_page)[0]
            else:
                raise Exception('E1')
        pass
        return res

    def liner_select(self, leaf_header, cond_list):
        """
        线性查找
        :param leaf_header:
        :param cond_list:
        :return:
        """
        res = []
        page = leaf_header
        while page != -1:
            addr = self.get_addr(page)
            page_res = self.select_record(addr, cond_list)
            res.extend(page_res)
            page = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.next_page)[0]
        pass
        return res

    def drop_tree(self, page_no, index_fmt):
        """
        删除以page_no为根节点的树
        :param page_no:
        :param index_fmt:
        :return:
        """
        stack = []
        stack.append(page_no)
        while len(stack) != 0:
            page_no = stack.pop()
            addr = self.get_addr(page_no)
            is_leaf = struct.unpack_from('?', self.pool.buf, (addr << 12) + Off.is_leaf)[0]

            if not is_leaf:
                p = struct.unpack_from('i', self.pool.buf, (addr << 12) + Off.header)[0]
                while p != 0:
                    r = struct.unpack_from(index_fmt, self.pool.buf, (addr << 12) + p + RecOff.record)
                    stack.append(r[0])
                    p = struct.unpack_from('i', self.pool.buf, (addr << 12) + p + RecOff.next_addr)[0]

            self.delete_buffer(page_no)
        return

    def create_tree(self, value_list, fmt, index_fmt):
        """
        创建一颗树，返回树的位置
        :param index_fmt:
        :param fmt:
        :param value_list:
        :return: 树的根节点
        """
        page_no = self.new_root(True, fmt)
        for value in value_list:
            new_page = self.insert_index(value, page_no, 0, index_fmt)
            if new_page != -1:
                page_no = new_page
        return page_no
