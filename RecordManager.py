from typing import ValuesView
from BufferManager import Page,load_buffer()
import re

def insert_record(page, value_lists, index_no):
    """
    在指定的页插入一条数据
    :param page: 列表，需要插入的页
    :param value_lists: 列表，需要插入的value
    :param index_no: 索引在value_list的第几个位置
    :return: 返回插入后的page，列表
    """
    user_record = page[Page.user_record]
    if len(user_record) == 0:
        value_list = value_lists.pop()
        user_record.append(value_list)
    for value_list in value_lists:
        for i in range(len(user_record) + 1):
            if i == len(user_record) or value_list[index_no] <= user_record[i][index_no]:
                user_record.insert(i, value_list)
                break
    page[Page.user_record] = user_record
    return page


# def delete_record(self, table_name, condition):
#     """
#     删除record
#     """
#     page_no=Index_Manager.select_page(self,table_name,condition)#首先应用索引找到page_no
#     page=self.read_buffer(page_no)#读出page
#     if len(page.user_record)==0:#如果读到的一页里面没有任何数据
#         return None
#     for record in range(0,len(page.user_record)):
#         if true(record,condition):#如果条件成立
#             page.user_record.remove(record)

def select_record_in_a_page(page_no,table_name, condition):#线性查找
    """
    在一个page中寻找record
    """
    #首先应用索引找到page_no
    page=load_buffer(page_no)#读出page
    if len(page.user_record)==0:#如果读到的一页里面没有任何数据
        return None
    recordlist=[]
    for record in range(0,len(page.user_record)):
        if true(record,condition):#如果条件成立
            recordlist.append(record)
    return recordlist


def delete_record(page, offset):
    """
    删除一条记录
    :param page:删除的记录所在的页
    :param offset: 删除记录的位置
    :return: page
    """
    user_record = page[Page.user_record]
    del user_record[offset]
    page[Page.user_record] = user_record
    return page


def _select_filter(records, col, op, value, record_cols, reverse=False):
    col_index = record_cols.index(col)
    value =eval(value)

    def f1(v):
        return v > value

    def f2(v):
        return v < value

    def f3(v):
        return v == value

    def f4(v):
        return v >= value

    def f5(v):
        return v <= value

    def f6(v):
        return v != value

    funcs = {'>': f1, '<': f2, '=': f3, '>=': f4, '<=': f5, '<>': f6}
    if op in funcs:
        f = funcs[op]
    else:
        print('Illegal operator: {}'.format(op))

    new_records = []
    for r in records:
        satisfy = f(r[col_index])
        if reverse:
            satisfy = not satisfy
        if satisfy:
            new_records.append(r)

    return new_records


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
    else:
        return True                    

def select_record(page_no,cols, table, condition, buf):## select cols from table   where condition    like: 
                                                ## select  *   from student where age>18

    page = load_buffer(page_no)#读catalog_buffer
    if table in page:
        record = page[table]
    else:
        print('table {} does not exit'.format(table))
    record_cols = record['column_list']
    record_types = record['fmt']
    indexlist = record['index_list']

    index_cols = []
    trees = []
    if len(indexlist) != 0:
        for col, index_name in indexlist:
            index_cols.append(col)
            tree = indexlist[index_name]#address为索引的名称+索引，表示所在树的根地址
            trees.append(tree)

    return_records = []

    # table_blocks = []
    # for i, b in enumerate(buf.header):
    #     if b['table'] == table:
    #         table_blocks.append(i)

    # select from all blocks
    if condition is not None:
        exps = condition.split('and')#将不同条件分隔开

        if len(indexlist) != 0 and len(exps) == 1:
            exp = exps[0].strip()
            match = re.match(r'^([A-Za-z0-9_]+)\s*([<>=]+)\s*(.+)$', exp, re.S)

            if match and len(match.groups()) == 3:
                col, op, value = match.groups()#col:

                if op == '=' and col in index_cols:
                    col_index = index_cols.index(col)
                    tree = trees[col_index]
                    value = _convert_to(value, record_types[col_index])
                    ptrs = tree.search(value)
                    if isinstance(ptrs, list):
                        for p in ptrs:
                            block_id = p // MAX_RECORDS_PER_BLOCK
                            pos = p % MAX_RECORDS_PER_BLOCK
                            b = buf.get_block(block_id)
                            return_records.append(b.data()[pos])
                    else:
                        block_id = ptrs // MAX_RECORDS_PER_BLOCK
                        pos = ptrs % MAX_RECORDS_PER_BLOCK
                        b = buf.get_block(block_id)
                        return_records.append(b.data()[pos])
                else:
                    return_records = _select_without_index(table_blocks, buf, exps, record_cols, record_types)

        else:
            return_records = _select_without_index(table_blocks, buf, exps, record_cols, record_types)

    else:
        for i in table_blocks:
            b = buf.get_block(i)
            records = b.data()
            return_records += records

    # select cols
    if cols != '*':
        indices = [record_cols.index(c) for c in cols]
        return_records = [[r[i] for i in indices] for r in return_records]
        return_cols = cols
    else:
        return_cols = record_cols

    log('select from {}'.format(table))

    return return_records, return_cols

def _select_without_index(table_blocks, buf, exps, record_cols, record_types):
    return_records = []
    for i in table_blocks:
        b = buf.get_block(i)
        records = b.data()

        for exp in exps:
            exp = exp.strip()
            match = re.match(r'^([A-Za-z0-9_]+)\s*([<>=]+)\s*(.+)$', exp, re.S)
            if match and len(match.groups()) == 3:
                col, op, value = match.groups()
                records = _select_filter(records, col, op, value, record_cols, record_types)
            else:
                raise MiniSQLSyntaxError('Illegal condition: {}'.format(exp))
        return_records += records
    return return_records

def _convert_to(v, t):
    if t == 0:
        return int(v)
    elif t == -1:
        return float(v)
    else:
        return v.strip("'")


def _select_filter(records, col, op, value, record_cols, record_types, reverse=False):
    col_index = record_cols.index(col)
    value = _convert_to(value, record_types[col_index])

    def f1(v):
        return v > value

    def f2(v):
        return v < value

    def f3(v):
        return v == value

    def f4(v):
        return v >= value

    def f5(v):
        return v <= value

    def f6(v):
        return v != value

    funcs = {'>': f1, '<': f2, '=': f3, '>=': f4, '<=': f5, '<>': f6}
    if op in funcs:
        f = funcs[op]
    else:
        print('Illegal operator: {}'.format(op))

    new_records = []
    for r in records:
        satisfy = f(r[col_index])
        if reverse:
            satisfy = not satisfy
        if satisfy:
            new_records.append(r)

    return new_records