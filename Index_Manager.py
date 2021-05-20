from Catalog_Manager import Catalog_Manager, Table
from Buffer_Manager import Buffer_Manager, Page
from Record_Manager import Record_Manager

class Index_Manager(Record_Manager):
    """
    主要是B+树索引的管理
    """
    def new_table(self, table_name, column_list, fmt_list, primary_key):
        """
        table_name: 表的名称
        column_list: dict{name, is_unique}
        primary_key: 主键
        """
        if primary_key is None:
            primary_key = column_list.keys[0]
        index_no = column_list[primary_key].column_no

        # 修改catalog_manager
        index_list = {primary_key:-1}
        table = Table(column_list, fmt_list, index_list,
                      -1, primary_key=primary_key)
        Catalog_Manager.catalog_buffer[table_name] = table
        self.new_root(table_name, True, primary_key, fmt_list.fmt, index_no)

    def delete_table(self, table_name):
        """
        删除record文件
        """
        pass

    def new_root(self, table_name, is_leaf, index, fmt, index_no):
        """
        创建根节点
        index       是索引的名称
        fmt         是根的解码方法
        """
        # 为tree增加一页
        page_no = self.new_buffer()
        page = Page(next_page=-1, current_page=page_no, parent=-1, is_leaf=is_leaf, index_no=index_no,
                    fmt=fmt, fmt_size=len(fmt), user_record=[], page_header=None)
        # 修改Catalog_Manager
        Catalog_Manager.catalog_buffer[table_name].index_list[index] = page_no
        Buffer_Manager.buffer_pool[page_no] = page
        if is_leaf:
            Catalog_Manager.catalog_buffer[table_name].page_header = page_no
        return page_no

    def delete_tree(self, table_name):
        pass

    def select_page(self, table_name, condition=None):
        """
        查找page
        """
        pass

    def insert_index(self, value_list, table_name, index):
        """
        从叶子插入，然后保持树
        table_name         当前操作的表
        index              插入的记录的索引，是主键还是其他索引
        value_list         需要插入的值
        root               根的page_no
        index_no           index在value_list第几个地方
        index_fmt          非叶子的解码格式，如果是主码，则是主码+i，如果是二级索引，则是索引+主码
        """
        catalog_buffer = Catalog_Manager.catalog_buffer[table_name]
        page_no = catalog_buffer.index_list[index]
        index_no = catalog_buffer.column_list[index].column_no
        index_fmt = catalog_buffer.fmt_list[index_no] + 'i'  # 索引的解码方式，需要i
        page = self.read_buffer(page_no)
        while not page.is_leaf:
            i = 0
            while value_list[index_no] < page.user_record[i][0] and i < len(page.user_record) - 1:
                i += 1
            page_no = page.user_record[i][1]
        value_list = [value_list]
        # 用主键插入
        is_full = self.insert_record(page_no, value_list, index_no)
        while is_full:
            n = len(page.user_record)
            user_record = page.user_record[n//2:-1]
            right_page_no = self.new_buffer()  # left是原来的
            left_page = Page(right_page_no, page.current_page, page.parent, page.fmt_size, page.index_no, page.is_leaf,
                              page.fmt, user_record, page_header=None, acquire_times=page.acquire_times)
            user_record = page.user_record[0:n//2]
            right_page = Page(page.next_page, right_page_no, page.parent, page.fmt_size, page.index_no, page.is_leaf,
                              page.fmt, user_record, page_header=None, acquire_times=page.acquire_times)
            Buffer_Manager.buffer_pool[page_no] = left_page
            Buffer_Manager.buffer_pool[right_page_no] = right_page
            if not right_page.is_leaf:
                for record in right_page.user_record:
                    self.read_buffer(record[1])
                    Buffer_Manager.buffer_pool[record[1]].parent = right_page_no
            if page.parent == -1:
                self.new_root(table_name, is_leaf=False, index=index, fmt=index_fmt, index_no=index_no)
            else:
                page_no = page.parent

            value_lists = [left_page.user_record[0], right_page.user_record[0]]
            is_full = self.insert_record(page_no, value_lists, 0)

    def delete_root(self):
        pass