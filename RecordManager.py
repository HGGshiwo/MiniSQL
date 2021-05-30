from BufferManager import Page


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
