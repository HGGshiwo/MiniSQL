# 数据库简介

使用了shared_memory共享数据，性能获得巨大提升！

但是shared_memory限制了存储方式，只能按照bytearray存放，需要进行select或者比较时再进行解码。

bytearray的存储，int占四个byte，所以int -1在bytearray中的形式是：0xff 0xff 0xff 0xff，占4个字节

char占1个字节，所以char r在bytearray中的形式是 0xb'r'，就是r的acsicc码

有关并行：和无并行本质上没有区别，唯一对我们的影响是：如上所言，限制了存储的形式。

因此，我们设计了一套较为巧妙的方式，能够将数据存放起来。

约定pool大小为256，约定表个数为256

# 数据操作

## 1 Page

page是一页，按照一个bytearray的形式存储。前面部分是page_header，即页信息。后面是用户数据。

### 1.1 在pool中获取page的例子

```python
if self.addr_list.count(page_no) == 0:  # 检查page_no是否在addr_list中
	self.load_page(page_no)  # 如果不在，则从文件中拷贝进来
addr = self.addr_list.index(page_no)  # 获得page_no在addr_list的第几个元素
true_addr = addr << 12 # page_no所在page的真实位置是addr*4096
# addr就是page的第一个byte所在的下标
```

### 1.2 解析page的例子

page是一段bytearray，其中的解析方式如下：

| 位置         | 名称              | 含义                                 | 宽度     | 不存在时 |
| ------------ | ----------------- | ------------------------------------ | -------- | -------- |
| 0:4          | current_page      | 当前页号                             | 4        | -1       |
| 4:8          | next_page         | 后一页                               | 4        | -1       |
| 8:12         | header            | 第一条记录的位置                     | 4        | 0        |
| 12:13        | ==is_leaf==       | 是否是叶子                           | 1        |          |
| 13:17        | ==previous_page== | 前一页                               | 4        | -1       |
| 17:21        | ==parent==        | 父页                                 | 4        | -1       |
| 21:25        | ==fmt_size==      | fmt所占字节数目                      | 4        |          |
| 25:k         | ==fmt==           | 字符串，表示解码格式，不包括两个指针 | fmt_size |          |
| k:k+fmt_size | user_record       | 用户记录                             |          | 0        |
| ...          |                   |                                      |          |          |

注意，python中表示的方式是[a:b]的含义是a到b-1，因此a[0:1]表示的就是a[0]，但是不能写成a[0]

### 1.3 对page_no的指定位赋值

```python
# 假如想要对page的next_page进行赋值, 注意next_page在addr开始后的几个byte之后。
struct.pack_into('i', self.pool.buf, (addr << 12) + Off.next_page, next_page)
# pack_into将一个数据转为byte写入内存
# i表示数据的格式是int
# self.pool.buf是写入的内存
# (addr << 12) + Off.next_page是偏移位置。addr是页的位置，Off.next_page是next_page相对页的位置
# next_page 想要写入的数据
```

### 1.4读取page_no的指定位

```python
next_page = struct.unpack_from('i', self.pool.buf, addr + Off.next_page)
```



### 1.5 解析record的例子

用户记录也是bytearray，是page的一段(slice)。确定一条记录的信息需要3个地址：```addr + p + RecOff.record```

addr是页所在的地址，详见1.1

p是记录相对于页所在的位置，第一条记录并不在0处，需要从page_header中获取。

RecOff.record是真实记录的偏移，它之前还有vaild，curreant_addr，next_addr，pre_addr

解析方式如下：

| 偏移位置     | 名称          | 含义             | 宽度 | 不存在时 |
| ------------ | ------------- | ---------------- | ---- | -------- |
| 0:1          | valid         | 记录是否有效     | 1    | 0        |
| 1:5          | current_addr  | 当前记录的地址   | 4    | 0        |
| 5:9          | next_addr     | 下一条记录的地址 | 4    | 0        |
| 9:13         | previous_addr | 上一条记录的地址 | 4    | 0        |
| 13:size(fmt) | record        | 用户记录         | ...  | 0        |

如果不存在，统一赋值为0

遍历page_no的所有record

```python
# 首先，从page_header中读出header
# addr是page_no的地址，计算方法见1
p = struct.unpack_from('i', self.pool.buf, addr + Off.header)[0] # p就是对于page来说，第一条记录的位置
while p != 0:
    # pool--page--record--value，value是真实的数据，record包含value和next_addr, pre_addr
    r = struct.unpack_from(fmt, self.pool.buf, addr + p + RecOff.record) 
    if r[index] > value_list[index]:
		# do something
    p = struct.unpack_from('i', self.pool.buf, addr + p + RecOff.next_addr)[0]
```

如果插入或者删除，不仅需要在物理空间将其vaild进行赋值，还需要维护链表

## 2 Table操作

### 2.1 获取表信息的例子

在table = self.table_list[table_name]中获取

### 2.2 解析表信息的例子

每一个表都单独使用一个列表。

一个table列表的信息：

| 位置 | 名称        | 含义                                                |
| ---- | ----------- | --------------------------------------------------- |
| 0    | primary_key | primary_key所在的位置下标                           |
| 1    | leaf_header | 第一个叶子节点所在的页号                            |
| 2    | name        | 列的名称，字符串                                    |
| 3    | fmt         | 字符串，第一个元素的解析方式                        |
| 4    | unique      | bool，第一个元素是否是unique                        |
| 5    | index_page  | int，第一个元素索引所在的根节点，如果不是索引则为-1 |
| 6    | name        |                                                     |
| 7    | fmt         |                                                     |
| 8    | unique      |                                                     |
| 9    | index_page  |                                                     |
| ...  | ....        |                                                     |

所以table的长度总是2+4*k，k是属性个数

### 2.3 修改表信息的例子

直接像数组一样读或者赋值即可，每一个位置的含义见上表



# 3 共享资源

共享资源全部是成员变量，可以用self进行访问

| 共享的资源名称      | 含义                                                         | 操作者       |
| ------------------- | ------------------------------------------------------------ | ------------ |
| addr_list           | 第i个页放的page_no是多少，如果被删除，则修改为-1             | load, unload |
| dirty_list          | 第i个页是否是脏页，默认被pin过就是脏页，需要自己把它修改为False | pin          |
| refer_list          | 第i个页的引用次数                                            | free         |
| occupy_list         | 第i个页被哪个进程占用，无则是-1                              | pin, unpin   |
| pool                | 一段bytearray                                                | 任意         |
| delete_list         | 一个delete_list，如果被删除的文件会记录在上面                |              |
| catalog_list        | 记录所有表的名称                                             |              |
| catalog_occupy_list | catalog被占用的情况                                          |              |
| table_name1         | 一个表的信息                                                 |              |
| table_name2         | 一个表的信息                                                 |              |
| ...                 | ...                                                          |              |
| user_name1          | 一个用户的信息                                               |              |
| user_name2          | 一个用户的信息                                               |              |
| ...                 | ...                                                          |              |



# 4 错误码

含义：T代表tabe，I代表Index，R代表record，E代表格式，B代表内存

| 错误代码 | 含义                                      |
| -------- | ----------------------------------------- |
| E1       | 错误的指令                                |
| E2       | 缺少参数                                  |
| E3       | 不支持的数据类型                          |
| B1       | 内存不足                                  |
| T1       | 表名已经存在                              |
| T2       | 表名不存在                                |
| R1       | 属性名不存在                              |
| R2       | 属性名已经存在                            |
| R3       | value个数不匹配                           |
| R4       | value格式不匹配                           |
| R5       | unique字段插入重复数据（这个怎么实现呀?） |
| I1       | 索引已经存在                              |
| I2       | 非unique建立索引                          |
| I3       | 主键                                      |



| 操作         | 可能的错误   |
| ------------ | ------------ |
| create table | T1, R2, R1   |
| drop table   | T2           |
| create index | I1,T2,R1,I2, |
| drop index   | T2, R1, I3   |
| insert       | T2,R3,R4,R5  |
| delete       | T2,E         |
| select       | T2           |



注意，不在interpreter里面发现异常，intrpreter只处理异常。打印结果。