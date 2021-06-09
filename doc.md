## 1 Page和Buffer操作

### 1.1 在buffer_pool中获取page的例子

比如想获得page_no对应的page,使用```read_buffer(share, page_no)```，返回一个列表。此时该page_no被pin住。

因此在unpin前不要重复调用read_buffer

另一种方式：```index = find_page(share, page_no)```，然后使用share.buffer_pool[index]进行访问



### 1.2 解析page的例子

Page一个列表下标的含义。具体定义见BufferManager

| 名称          | 含义             |
| ------------- | ---------------- |
| next_page     | 下一页的位置     |
| current_page  | 当前页的位置     |
| previous_page | 前一页的位置     |
| parent        | 父页的位置       |
| fmt_size      | fmt大小          |
| index_offset  | 索引在第几个位置 |
| is_leaf       | 是否是叶子       |
| fmt           | 一段字符串       |
| is_delete     | 是否被删除       |
| is_change     | 是否被修改       |
| user_record   | 列表的数据       |

比如获得page后，想获得next_page，则用``next_page = page[Page.next_page]``

注意，如果next_page或者其他不存在，那么默认值是-1



### 1.3 写回page的例子

假如page存在buffer_pool中，且被pin住，则通过find_page(share, page_no)计算index

用```fresh_buffer(share, page, index)```写回

写回后自动unpin，所以不要读一次，写回多次



## 2 Table操作

### 2.1 获取表信息的例子

如果想获取一个表名是table_name的表的信息，使用```read_catalog(share, table_name)```获取。

得到的是一个列表table_info

注意，读table_info后，该表的信息就被pin住，因此在unpin前不要重复使用```read_catalog```，会导致卡住。

### 2.2 解析表信息的例子

table_info是一个列表，记录表的信息，第一项是column_list，可以用table[Table.column_list]来访问，顺序见下表

| 名称b       | 含义                              |
| ----------- | --------------------------------- |
| column_list | 列名的列表                        |
| fmt_list    | 列解码方式的列表                  |
| index_list  | 字典{index:page_no}，记录根的位置 |
| unique_list | unique的列所在的位置              |
| page_header | int 第一个数据页的位置            |
| primary_key | 主键                              |

### 2.3 修改表信息的例子

当修改一个表之后，使用```fresh_catalog(share, table_name, table)```写入share的catalog_info中。table是一个列表，和上面获取的一样。

一次只允许修改一个表的内容。

修改以后，调用```unpin(share, 'catalog.'+ table_name)```把表的信息解锁



==尽量使用读写函数进行读写,另，buffer_pool的索引已经不是page_no，因此不能用buffer_pool[page_no]进行访问==





