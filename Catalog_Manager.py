import copy

from Buffer_Manager import Buffer_Manager


class Table(object):
    def __init__(self, column_list, fmt_list, index_list, page_header, primary_key):
        self.column_list = column_list
        self.fmt_list = fmt_list
        self.index_list = index_list
        self.page_header = page_header
        self.primary_key = primary_key


class Column(object):
    def __init__(self, is_unique, column_no):
        self.is_unique = is_unique
        self.column_no = column_no


class Fmt_List(list):
    @property
    def fmt(self):
        fmt = ''
        for f in self:
            fmt = fmt + f
        return fmt


class Catalog_Manager(Buffer_Manager):
    catalog_buffer = {}
    user_buffer = {}
    privilege_buffer = {}

    def __init__(self):
        Buffer_Manager.__init__(self)

        catalog_buffer = self.read_json('catalog')
        for table_name in catalog_buffer.keys():
            column_list = catalog_buffer[table_name]['column_list']
            fmt_list = Fmt_List(tuple(catalog_buffer[table_name]['fmt_list']))
            index_list = catalog_buffer[table_name]['index_list']
            page_header = catalog_buffer[table_name]['page_header']
            primary_key = catalog_buffer[table_name]['primary_key']
            c_list = {}
            for column in column_list.keys():
                c = Column(column_list[column]['is_unique'], column_list[column]['column_no'])
                c_list[column] = c
            table = Table(c_list, fmt_list, index_list, page_header, primary_key)
            Catalog_Manager.catalog_buffer[table_name] = table

        Catalog_Manager.user_buffer = self.read_json('user')
        Catalog_Manager.privilege_buffer = self.read_json('privilege')

    def sys_exit(self):
        """
        相当于析构函数
        """
        catalog_buffer = {}
        for table_name in Catalog_Manager.catalog_buffer.keys():
            table = Catalog_Manager.catalog_buffer[table_name]
            column_list = {}
            for column_name in table.column_list.keys():
                column = table.column_list[column_name]
                column_list[column_name] = {'column_no': column.column_no, 'is_unique': column.is_unique}
            fmt_list = list(tuple(table.fmt_list))
            index_list = table.index_list
            page_header = table.page_header
            primary_key = table.primary_key
            catalog_buffer[table_name] = \
                {'column_list':column_list, 'fmt_list':fmt_list,
                 'index_list':index_list, 'page_header':page_header, 'primary_key':primary_key}
        self.write_json('catalog', catalog_buffer)
        self.write_json('user', Catalog_Manager.user_buffer)
        self.write_json('privilege', Catalog_Manager.privilege_buffer)

        buffer = {}
        buffer["del_header"] = Buffer_Manager.del_header
        buffer['heap_top'] = Buffer_Manager.heap_top
        self.write_json('buffer', buffer)
        key_list = copy.copy(list(Buffer_Manager.buffer_pool.keys()))
        for page_no in key_list:
            self.unload_buffer(page_no)