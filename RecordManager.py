from BufferManager import Page
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

# def select_record(self, page_no,table_name, condition):#线性查找
#     """
#     在一个page中寻找record
#     """
#     page_no=Index_Manager.select_page(self,table_name,condition)#首先应用索引找到page_no
#     page=self.read_buffer(page_no)#读出page
#     if len(page.user_record)==0:#如果读到的一页里面没有任何数据
#         return None
#     recordlist=[]
#     for record in range(0,len(page.user_record)):
#         if true(record,condition):#如果条件成立
#             recordlist.append(record)
#     return recordlist


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
