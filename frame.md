### BufferManager 

```
__init__(self, lock_list):
```

初始化BufferManager：初始化*self*.lock_list，对于*self*.pool 、*self*.addr_list 、*self*.dirty_list 、*self*.refer_list、*self*.file_list在shareablememory中进行初始化

```
find_space(self, page_no):
"""
为page_no在pool申请空间， 并在addr_list中注册
:param page_no: 想要放入的页号
:return: page_no在pool中的地址
"""
```

空间申请 ：若有空闲位置则返回最靠前的index；若无空闲位置，则用时钟算法删除一页，返回相应的index

```
load_page(self, page_no):
"""
把文件加载入缓存
:param page_no: 指定的页号
:return: 返回写入的地址
"""
```

载入缓存

```
new_buffer(self):
 """
申请文件和对应的page_no, 将其加入缓存中，得到一个addr，将其pin住
:param self:
:return: 返回申请得到的地址addr, 页号page_no
"""
```

申请文件地址与页号，若无空余则提示内存不足(B1)

```
delete_buffer(self, page_no):
"""
将缓存中的page_no删除，将文件删除
:param self:
:param page_no:待删除的页号
:return:None
"""
```

按页号删除文件

```
unload_buffer(self, addr):
"""
将一个page写回文件
:param addr: 地址
:return: None
"""
```

将指定地址上的page写回文件

```
get_addr(self, page_no):
"""
寻找page_no的地址
:param page_no:待查找的页号
:return: addr 地址
"""
```

寻找page_no的地址

```
quit_buffer(self):
```

退出buffer：将buffer写入文件，在shareablememory中关闭、*self*.pool 、*self*.addr_list 、*self*.dirty_list 、*self*.refer_list、*self*.file_list

### Catalogmanager

```
__init__(self, table_lock = None):
```

初始化CatalogManager：初始化*self*.table_lock，对于 *self*.catalog_list、*self*.table_list在shareablememory中进行初始化

```
new_table(self, table_name, primary_key, table_info, page_no):
"""
新建一个表的信息
:param table_name:表名
:param primary_key:主键在表中的位置
:param table_info:表的信息，按照“索引名、fmt、是否unique、元素索引所在根节点位置（非索引则为-1）”的顺序，逐个元素排列
:param page_no:储存表的页号
:return:
"""
```

新建表：在catalog_list中注册该表，并将该表的信息写入内存。若catalog_list已满则提示内存不足(B1)，若已存在同名表则提示表名已经存在(T1)。

```
get_table(self, table_name):
"""
获得表
:param table_name:表名
:return:表，即self.table_list[table_name]
"""
```

根据表名获得table_list中的表。若表名不存在则提示表名不存在(T2)。

```
delete_table(self, table_name):
"""
删除表信息
:param table_name:表名
:return:None
"""
```

删除相应表名的信息，在*self*.catalog_list、*self*.table_list中进行修改

```
quit_catalog(self):
"""
退出表管理
:return:None
"""
```

退出表管理：在文件中写入*self*.catalog_list中储存的表的信息，在shareablememory中关闭 *self*.table_list、*self*.catalog_list

### RecordManager

```
insert_record(self, addr, record, index):
"""
在指定的index的页中插入一条数据，如果已满，则删除最后一条数据，然后插入新数据，然后返回
:param index: 以第几个属性为索引进行插入
:param addr:地址
:param record:插入的数据
:return:返回 head_value, record
        分别表示该节点头需要修改的值，该节点最后一条记录。
        如果不需要修改，也未满，则返回None
"""
```

插入记录：根据addr寻找物理地址插入数据；寻找逻辑地址，对前后记录的指针进行维护

```
delete_record(self, addr, index, value):
"""
删除指定索引值的记录
:param value: 索引值是多少
:param index: 索引是哪一个
:param addr: 页的地址
:return:should_merge，是否需要合并，head_value, 当前第一条记录的值，如果不需要修改返回None
"""
```

删除记录：根据addr寻找物理地址，删除指定index的记录，通过物理地址检测是否需要合并

```
count_valid(self, addr):
"""
统计一页中有几条有效记录
:param addr:待统计的地址
:return:valid_num, invalid_num 有效记录和无效记录的数目
"""
```

统计一页中有几条有效记录

```
select_record(self, addr, cond_list, index_cond=None):
"""
在一页中按照条件选择记录,
:param index_cond: 索引使用的条件
:param addr:页的地址
:param cond_list:其他的条件
:return:符合条件的记录列表 res
"""
```

按照条件选择记录：调用check_cond对一页中的记录进行判断，调用go_ahead函数对是否继续查询进行判断

```
check_cond(r, cond_list):
"""
对record进行检测
:param r:待检测的记录
:param cond_list:检测的条件
:return: True/False
"""
```

```
go_ahead(r, index_cond):
"""
判断是否对下一条记录进行检查，在一页中总是从左往右检查的
:param r:待检测的记录
:param index_cond:索引使用的条件
:return:True/False
"""
```

### Indexmanager

```
__init__(self, lock_list=None):
```

初始化Indexmanager：调用RecordManager的初始化函数

```
new_root(self, is_leaf, fmt):
"""
创建新的根节点，不会在catalog中修改，因此需要外部修改catalog
会直接写入buffer_pool中，可以从buffer_pool中获得该页
:param is_leaf: 是否是叶子
:param fmt: 新的根解码形式
:return:新根所在的page_no
"""
```

创建根节点，返回新根所在的page_no

```
insert_index(self, value_list, page_no, index, index_fmt):
"""
从叶子插入，然后保持树
:param page_no: 根节点所在的页号
:param index_fmt: 索引格式
:param value_list: 一条记录
:param index: 当前的索引是第几个
:return: 新的根，如果被修改的话，否则是-1
"""
```

插入到叶节点：根据page_no得到根节点位置，循环到叶子节点，调用RecordManager中的insert_record函数插入记录，调用replace_value函数维护维护head_value，维护B+树，返回新的根所在的page_no

```
def delete_index(self, page_no, index, index_fmt, value):
"""
当找到一条记录后，主索引值，其他索引值都知道了，因此可以执行删除索引值是value的记录
:param page_no:根节点所在的页号
:param index_fmt:索引格式
:param index:当前的索引是第几个
:param value:索引值
:return: leaf header, index page 叶子头节点指针 新根所在的page_no
"""
```

删除指定叶节点：根据page_no得到根节点位置，循环到叶子节点，调用RecordManager中delete_record函数删除记录，调用replace_value函数维护维护head_value，维护B+树，返回叶子头节点指针、 新根所在的page_no

```
replace_value(self, page_no, index_fmt, head_value):
"""
在page_no的父节点，把page_no对应的value替换为head_value，一直到根
:param page_no:根节点所在的页号
:param index_fmt:索引格式
:param head_value: 想要修改成的值
:return:None
"""
```

向上维护B+树的父节点

```
select_page(self, page_no, index_fmt, index_cond, cond_list):
"""
在指定根页的树中查找
:param index_cond:索引使用的条件
:param index_fmt:索引格式
:param page_no:根节点所在的页号
:param cond_list:列表,放不同的语句，类似于[[0, '=', 2],[1, '>', 3]]
:return:符合条件的记录列表 res
"""
```

在指定根页的树中查找：根据page_no得到根节点位置，循环到叶子节点，调用RecordManager中的select_record函数查找记录，返回符合条件的记录列表 res

```
liner_select(self, leaf_header, cond_list):
"""
线性查找
:param leaf_header:根节点所在的页号
:param cond_list:列表,放不同的语句，类似于[[0, '=', 2],[1, '>', 3]]
:return:符合条件的记录列表 res
"""
```

线性查找用于未建立索引的情况，逐页调用RecordManager中的select_record函数查找记录，返回符合条件的记录列表 res

```
drop_tree(self, page_no, index_fmt):
"""
删除以page_no为根节点的树
:param page_no:根节点所在的页号
:param index_fmt:索引格式
:return:None
"""
```

调用BufferManager中的delete_buffer函数删除以page_no为根节点的树

```
create_tree(self, value_list, fmt, index_fmt):
"""
创建一颗树，返回树的位置
:param index_fmt:索引格式
:param fmt:根解码形式
:param value_list:一条记录
:return: 树的根节点
"""
```

调用new_root函数创建根节点，调用insert_index函数插入根节点记录

### Api

```
__init__(self, lock_list=None):
```

初始化API：初始化grant_list，并调用IndexManager和CatalogManager的初始化函数

```
create_table(self, table_name, primary_key, table_info):
"""
为新表格在catalog中注册
:param table_name:表名
:param primary_key:主键在表中的位置
:param table_info: 顺序是name, fmt, unique, index_page。其中index_page全部是-1
:return:None
"""
```

创建表语句的实现：为新表格在catalog中注册，调用IndexManager的new_root函数和CatalogManager中的new_table函数

```
delete(self, table_name, condition):
"""
删除满足condition的节点
:param table_name:表名
:param condition:索引条件
:return:None
"""
```

删除记录语句的实现：删除满足condition的节点，调用select函数，CatalogManager中的get_table函数，IndexManager中的delete_index函数

```
insert(self, table_name, value_list):
"""
插入一条数据
:param table_name:表名
:param value_list:一条数据
:return:None
"""
```

插入记录语句的实现：同时更新主索引树和二级索引树，CatalogManager中的get_table函数，IndexManager中的insert_index函数

```
select(self, table_name, cond_list):
"""
直接进行查找，二级索引叶子中r[0]是索引值，r[1]是主键值
:param cond_list:索引条件
:param table_name:表名
:return:符合条件的记录列表 res
"""
```

选择语句的实现：会根据有无索引选择索引查找或线性查找，调用CatalogManager中的get_table函数，IndexManager中的select_page、liner_select函数

```
create_index(self, table_name, index):
"""
创建二级索引树
:param table_name:表名
:param index:索引位置
:return:None
"""
```

创建索引语句的实现：调用CatalogManager中的get_table、create_tree，get_addr函数

```
drop_index(self, table_name, index):
"""
删除指定索引
:param table_name:表名
:param index:索引位置
:return:None
"""
```

删除索引语句的实现：调用CatalogManager中的get_table、BufferManager中的drop_tree函数

```
drop_table(self, table_name):
"""
删除一个表
:param table_name:表名
:return:None
"""
```

删除表语句的实现：调用CatalogManager中的get_table、drop_tree、delete_table函数

```
quit(self):
```

退出MiniSQL系统语句的实现：调用quit_catalog和quit_buffe函数

### InterpreterManager

```
__init__(self):
```

初始化InterpreterManager：调用Api和Cmd的初始化函数

```
do_execfile(self, args):
```

执行SQL脚本文件语句的实现：读入指定位置的文件，拆分语句，并向其他函数传入规范化变量

```
do_select(self, args):
do_create(self, args):
do_drop(self, args):
do_insert(self, args):
do_delete(self, args):
do_quit(self,args):
default(self, line):
```

对于以select/create/drop/insert/delete/quit为首的语句，会传入上述对应函数中进行变量拆分，并传入Api中进行下一步操作，对于语句中可检查出的错误会给出相应的报错指令；其他语句不响应

```
print_record(data_list):
"""
友好化输出记录
:param data_list: 第一个是列名，然后是记录
:return:None
"""
```

对select返回的结果进行友好化输出



附录：源码 & 错误码 & 输出示例？