## Page

page是一段dict

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
| user_record   | 列表的数据       |

## Table

table就是一段dict

| 名称        | 含义                              |
| ----------- | --------------------------------- |
| column_list | 列名的列表                        |
| fmt_list    | 列解码方式的列表                  |
| index_list  | 字典{index:page_no}，记录根的位置 |
| unique_list | unique的列所在的位置              |
| page_header | int 第一个数据页的位置            |
| primary_key | 主键                              |

## create table


| 执行顺序   | 对共享资源的操作                                |
| ---------- | ----------------------------------------------- |
| new_table  | pin catalog_info                                |
| new_root   |                                                 |
| new_buffer | pin buffer_info, pin page_no, unpin_buffer_info |
| new_root   | fresh page_no的buffer                           |
| new_table  | 修改catalog_info, unpin catalog_info            |

## insert

| 执行顺序      | 操作                          |
| ------------- | ----------------------------- |
| insert_index  | pin catalog_info, pin page_no |
| insert_record |                               |
| insert_index  | pin new_page                  |
|               |                               |

