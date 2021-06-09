from IndexManager import TabOff
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

    def true(self,record,table_name,condition=None):#判断record是否满足其中的table符合condition，并输出符合的cols，比如true(record,sname,student,age>18)就是搜索record里面的student表当中符合age>18的，输出sname列。
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
def delete_table(table):
    # delete table instance
    tables.pop(table)

def delete_from_table(table,statement):
    # delete rows from table according to the statement's condition
    # usage : find_leaf_place_with_condition(table, column, value,condition)
    if len(statement) == 0:
        tables[table] = node(True,[],[],'')
        print("Successfully delete all entrys from table '%s'," % table,end='')
    else:
        columns = {}
        for index,col in enumerate(CatalogManager.catalog.tables[table].columns):
            columns[col.column_name] = index
        __primary_key = CatalogManager.catalog.tables[table].primary_key
        # __primary_key = 0
        # columns = {'num':0,'val':1}

        conditions = []
        tmp = []
        pos = 1
        for i in statement:
            if i == 'and':
                conditions.append(tmp)
                tmp = []
                pos = 1
                continue
            if pos == 1:
                tmp.append(columns[i])
            elif pos == 3:
                if CatalogManager.catalog.tables[table].columns[tmp[0]].type == 'char':
                    tmp.append(i.strip().replace("'", ''))
                elif CatalogManager.catalog.tables[table].columns[tmp[0]].type == 'int':
                    tmp.append(int(i))
                elif CatalogManager.catalog.tables[table].columns[tmp[0]].type == 'float':
                    tmp.append(float(i))
            else:
                tmp.append(i)
            pos = pos + 1
        conditions.append(tmp)
        times = 0
        while True:
            nodes = find_leaf_place_with_condition(table,
                            conditions[0][0],conditions[0][2],conditions[0][1])
            for col in conditions:
                if col[0] == __primary_key:
                    nodes = find_leaf_place_with_condition(table,col[0],col[2],col[1])
                    break

            if len(nodes) == 0:
                break
            seed = False
            for __node in nodes:
                if seed == True:
                    break
                for index,leaf in enumerate(__node.pointers[0:-1]):
                    if check_conditions(leaf,conditions):
                        __node.pointers.pop(index)
                        __node.keys.pop(index)
                        maintain_B_plus_tree_after_delete(table,__node)
                        times = times + 1
                        seed = True
                        break
            if seed == False:
                break
        print("Successfully delete %d entry(s) from table '%s'," % (times,table),end='')
def maintain_B_plus_tree_after_delete(table,__node):
    global N
    if __node.parent == '' and len(__node.pointers) == 1:
        tables[table] = __node.pointers[0]
    elif ((len(__node.pointers) < math.ceil(N/2) and __node.is_leaf == False) or
         (len(__node.keys) < math.ceil((N-1)/2) and __node.is_leaf == True) ) \
            and __node.parent != '':
        previous = False
        other_node = node(True,[],[])
        K = ''
        __index = 0
        for index, i in enumerate(__node.parent.pointers):
            if i == __node:
                if index == len(__node.parent.pointers) - 1:
                    other_node = __node.parent.pointers[-2]
                    previous = True
                    K = __node.parent.keys[index - 1]
                else:
                    K = __node.parent.keys[index]
                    other_node = __node.parent.pointers[index + 1]
                    __index = index + 1

        if (other_node.is_leaf == True and len(other_node.keys)+len(__node.keys) < N) or \
           (other_node.is_leaf == False and len(other_node.pointers) +
            len(__node.pointers) <= N):
            if previous == True:
                if other_node.is_leaf == False:
                    other_node.pointers = other_node.pointers + __node.pointers
                    other_node.keys = other_node.keys + [K] + __node.keys
                    for __node__ in __node.pointers:
                        __node__.parent = other_node
                else:
                    other_node.pointers = other_node.pointers[0:-1]
                    other_node.pointers = other_node.pointers + __node.pointers
                    other_node.keys = other_node.keys + __node.keys
                __node.parent.pointers = __node.parent.pointers[0:-1]
                __node.parent.keys = __node.parent.keys[0:-1]
                maintain_B_plus_tree_after_delete(table,__node.parent)
            else:
                if other_node.is_leaf == False:
                    __node.pointers = __node.pointers + other_node.pointers
                    __node.keys = __node.keys + [K] + other_node.keys
                    for __node__ in other_node.pointers:
                        __node__.parent = __node
                else:
                    __node.pointers = __node.pointers[0:-1]
                    __node.pointers = __node.pointers + other_node.pointers
                    __node.keys = __node.keys + other_node.keys
                __node.parent.pointers.pop(__index)
                __node.parent.keys.pop(__index-1)
                maintain_B_plus_tree_after_delete(table,__node.parent)
        else:
            if previous == True:
                if other_node.is_leaf == True:
                    __node.keys.insert(0,other_node.keys.pop(-1))
                    __node.pointers.insert(0,other_node.pointers.pop(-2))
                    __node.parent.keys[-1] = __node.keys[0]
                else:
                    __tmp = other_node.pointers.pop(-1)
                    __tmp.parent = __node
                    __node.pointers.insert(0,__tmp)
                    __node.keys.insert(0,__node.parent.keys[-1])
                    __node.parent.keys[-1] = other_node.keys.pop(-1)
            else:
                if other_node.is_leaf == True:
                    __node.keys.insert(-1,other_node.keys.pop(0))
                    __node.pointers.insert(-2,other_node.pointers.pop(0))
                    __node.parent.keys[__index-1] = other_node.keys[0]
                else:
                    __tmp = other_node.pointers.pop(0)
                    __tmp.parent = __node
                    __node.pointers.insert(-1,__tmp)
                    __node.keys.insert(-1,__node.parent.keys[__index-1])
                    __node.parent.keys[__index-1] = other_node.keys.pop(0)
                    
def check_conditions(leaf,conditions):
    for cond in conditions:
        # cond <-> column op value
        __value = leaf[cond[0]]
        if cond[1] == '<':
            if not (__value < cond[2]):
                return False
        elif cond[1] == '<=':
            if not (__value <= cond[2]):
                return False
        elif cond[1] == '>':
            if not (__value > cond[2]):
                return False
        elif cond[1] == '>=':
            if not (__value >= cond[2]):
                return False
        elif cond[1] == '<>':
            if not (__value != cond[2]):
                return False
        elif cond[1] == '=':
            if not (__value == cond[2]):
                return False
        else:
            raise Exception("Index Module : unsupported op.")
    return True
def select_from_table(table,__conditions = '',__columns = ''):
    results = []
    columns = {}
    for index,col in enumerate(CatalogManager.catalog.tables[table].columns):
        columns[col.column_name] = index
    __primary_key = CatalogManager.catalog.tables[table].primary_key
    # __primary_key = 0
    # columns = {'num': 0, 'val': 1}

    if len(tables[table].keys) == 0:
        pass
    else:
        if __conditions != '':
            conditions = []
            statement = __conditions.split(' ')
            tmp = []
            pos = 1
            for i in statement:
                if i == 'and':
                    conditions.append(tmp)
                    tmp = []
                    pos = 1
                    continue
                if pos == 1:
                    tmp.append(columns[i])
                elif pos == 3:
                    if CatalogManager.catalog.tables[table].columns[tmp[0]].type == 'char':
                        tmp.append(i.strip().replace("'",''))
                    elif CatalogManager.catalog.tables[table].columns[tmp[0]].type == 'int':
                        tmp.append(int(i))
                    elif CatalogManager.catalog.tables[table].columns[tmp[0]].type == 'float':
                        tmp.append(float(i))
                else:
                    tmp.append(i)
                pos = pos + 1
            conditions.append(tmp)
            nodes = find_leaf_place_with_condition(table,
                    conditions[0][0], conditions[0][2], conditions[0][1])
            for col in conditions:
                if col[0] == __primary_key:
                    nodes = find_leaf_place_with_condition(table, col[0], col[2], col[1])
                    break
            for __node in nodes:
                for pointer in __node.pointers[0:-1]:
                    if check_conditions(pointer,conditions):
                        results.append(pointer)
        else:
            first_leaf_node = tables[table]
            while first_leaf_node.is_leaf != True:
                first_leaf_node = first_leaf_node.pointers[0]
            while True:
                for i in first_leaf_node.pointers[0:-1]:
                    results.append(i)
                if first_leaf_node.pointers[-1] != '':
                    first_leaf_node = first_leaf_node.pointers[-1]
                else:
                    break

    if __columns == '*':
        __columns_list = list(columns.keys())
        __columns_list_num = list(columns.values())
    else:
        __columns_list_num = [columns[i.strip()] for i in __columns.split(',')]
        __columns_list = [i.strip() for i in __columns.split(',')]

    print('-' * (17 * len(__columns_list_num) + 1))
    for i in __columns_list:
        if len(str(i)) > 14:
            output = str(i)[0:14]
        else:
            output = str(i)
        print('|',output.center(15),end='')
    print('|')
    print('-' * (17 * len(__columns_list_num) + 1))
    for i in results:
        for j in __columns_list_num:
            if len(str(i[j])) > 14:
                output = str(i[j])[0:14]
            else:
                output = str(i[j])
            print('|',output.center(15) ,end='')
        print('|')
    print('-' * (17 * len(__columns_list_num) + 1))
    print("Returned %d entrys," % len(results),end='')
def check_unique(table,column,value):
    columns = []
    for col in CatalogManager.catalog.tables[table].columns:
        columns.append(col)
    if len(find_leaf_place_with_condition(table,column,value,'=')):
        raise Exception("Index Module : column '%s' does not satisfy "
                                "unique constrains." % columns[column])

def find_leaf_place(table,value):
    # search on primary key
    cur_node = tables[table]
    while not cur_node.is_leaf:
        seed = False
        for index,key in enumerate(cur_node.keys):
            if key > value:
                cur_node = cur_node.pointers[index]
                seed = True
                break
        if seed == False:
            cur_node = cur_node.pointers[-1]
    return cur_node

def find_leaf_place_with_condition(table,column,value,condition):
    # __primary_key = CatalogManager.catalog.tables[table].primary_key
    __primary_key = 0
    head_node = tables[table]
    first_leaf_node = head_node
    while first_leaf_node.is_leaf != True:
        first_leaf_node = first_leaf_node.pointers[0]
    lists = []

    if __primary_key == column and condition != '<>':
        while not head_node.is_leaf:
            seed = False
            for index, key in enumerate(head_node.keys):
                if key > value:
                    head_node = head_node.pointers[index]
                    seed = True
                    break
            if seed == False:
                head_node = head_node.pointers[-1]
        if condition == '=':
            for pointer in head_node.pointers[0:-1]:
                if pointer[column] == value:
                    lists.append(head_node)
        elif condition == '<=':
            cur_node = first_leaf_node
            while True:
                if cur_node != head_node:
                    lists.append(cur_node)
                    cur_node = cur_node.pointers[-1]
                else:
                    break
            for pointer in head_node.pointers[0:-1]:
                if pointer[column] <= value:
                    lists.append(head_node)
                    break
        elif condition == '<':
            cur_node = first_leaf_node
            while True:
                if cur_node != head_node:
                    lists.append(cur_node)
                    cur_node = cur_node.pointers[-1]
                else:
                    break
            for pointer in head_node.pointers[0:-1]:
                if pointer[column] < value:
                    lists.append(head_node)
                    break
        elif condition == '>':
            for pointer in head_node.pointers[0:-1]:
                if pointer[column] > value:
                    lists.append(head_node)
                    break
            while True:
                head_node = head_node.pointers[-1]
                if head_node != '':
                    lists.append(head_node)
                else:
                    break
        elif condition == '>=':
            for pointer in head_node.pointers[0:-1]:
                if pointer[column] >= value:
                    lists.append(head_node)
                    break
            while True:
                head_node = head_node.pointers[-1]
                if head_node != '':
                    lists.append(head_node)
                else:
                    break
        else:
            raise Exception("Index Module : unsupported op.")

    else:
        while True:
            for pointer in first_leaf_node.pointers[0:-1]:
                if condition == '=':
                    if pointer[column] == value:
                        lists.append(first_leaf_node)
                        break
                elif condition == '<':
                    if pointer[column] < value:
                        lists.append(first_leaf_node)
                        break
                elif condition == '<=':
                    if pointer[column] <= value:
                        lists.append(first_leaf_node)
                        break
                elif condition == '>':
                    if pointer[column] > value:
                        lists.append(first_leaf_node)
                        break
                elif condition == '>=':
                    if pointer[column] >= value:
                        lists.append(first_leaf_node)
                        break
                elif condition == '<>':
                    if pointer[column] != value:
                        lists.append(first_leaf_node)
                        break
                else:
                    raise Exception("Index Module : unsupported op.")
            if first_leaf_node.pointers[-1] == '':
                break
            first_leaf_node = first_leaf_node.pointers[-1]
    return lists


def select_record(self, table_name, condition=None):
    """
    查找page**********************************
    从根节点开始，如果符合条件那么返回来符合条件的索引；如果不符合条件那么继续进行
    顺序查询，一个一个找
    """
    self.pin_catalog(table_name)
    table=self.table_list[table_name]#获取表信息
    page=table[1]#到第一个叶子节点所在的地方
    indexlist=[]
    while page[4:8] != -1:#不是最右边的那一页
        user_record=page[Page.user_record]
        for item in user_record:
            if true(item,table_name,condition):#如果item符合condition
                indexlist.append(item)
    return indexlist
    
