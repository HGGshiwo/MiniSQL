# MiniSQL说明文档v2

# 1 概论

注意，如果确定了函数传递的参数，请填充在说明文档的相应位置。

主要修改的地方：

1.表的列和其他信息放在了```db_files/catalog.json```中，这是根据指导文件修改的

2.用户和权限放在了```db_files```下的对应文件中

3.表信息提供的接口是：```self.read_catalog```，返回字典。```self.write_catalog(catalog)```写入，此时是为了管理缓存方便

4.如果read_catalog之后对catalog产生了改变，一定要写入

5.所有的类如果读写文件，必须用读写函数：```self.write_index```,```self.read_index```,```self.write_data```,```self.read_data```

因为需要缓存管理，必须把读到的内容放入缓存

6.index的文件增加后缀json，二进制文件增加后缀dat，主要是为了查看方便

7.文件的结构变成了db_files/index/table_name和db_files/data/table_name

如果根据原来的设计写的，可以继续下去，最后修改也行。

# 2 类

类的关系

| 类名                | 解释                                                         |
| ------------------- | ------------------------------------------------------------ |
| main_buffer_manager | 基类，主线程                                                 |
| buffer_manager      | 提供了文件读写，如果需要读写文件的类必须继承它               |
| catalog_manager     | 继承了buffer_manager，提供了修改表的模式。如果修改表的模式（列名，索引之类的），必须继承它 |
| index_manager       | 继承了catalog_manager，主要是索引文件的修改                  |
| record_manager      | 继承了catalog_manager，主要是data文件的修改                  |
| interpreter         | 接受输入，反馈输出，语法检查，和api交互                      |
| api                 | 继承了index_manager和record_manager，调用index_manager和record_manager的函数 |

index_manager和record_manager其实关系很紧密，可能需要相互传递参数，相关的函数参数不统一规定

## 2.1 buffer_manager 类

基类

指导书描述：Buffer Manager负责缓冲区的管理，主要功能有：

1. 根据需要，读取指定的数据到系统缓冲区或将缓冲区中的数据写出到文件

2. 实现缓冲区的替换算法，当缓冲区满时选择合适的页进行替换

3. 记录缓冲区中各页的状态，如是否被修改过等

4. 提供缓冲区页的pin功能，及锁定缓冲区的页，不允许替换出去

为提高磁盘I/O操作的效率，缓冲区与文件系统交互的单位是块，块的大小应为文件系统与磁盘交互单位的整数倍，一般可定为4KB或8KB。

| 属性        | 含义                                                    |
| ----------- | ------------------------------------------------------- |
| data_buffer | 用来表示缓存，字典的字典，base:{data,times}，data是列表 |
| max_num     | 最大缓存页数                                            |

| 方法        | 含义                                           |
| ----------- | ---------------------------------------------- |
| write_data  | 添加一条记录，在缓存和文件中同时写入           |
| read_data   | 如果命中，则读取，未命中则写入                 |
| write_index | 写入缓存和文件                                 |
| read_index  | 从缓存中读取，如果缓存中不存在，则从文件中读取 |

| read_data参数表 | 含义                                                         |
| --------------- | ------------------------------------------------------------ |
| fmt             | 字符串，读取数据的解包类型，不含结尾的bool，和create里的fmt相同 |
| column_list     | 列表，列名的列表                                             |
| address         | 字典，{"base","offset"}，表示文件地址和第几条数据，如果offset=None，则返回所有数据 |

访问catalog是没关系的因为已经阻塞了

但是写入catalog时需要调用函数，因为可能需要删除页

```
data_buffer={
	base1:{times, data},
	base2:{times, data},
	....
}
```

data是列表，value_list的列表



## 2.2 catalog_manager 类

继承buffer_manager，所以可以使用buffer_manager的读写函数

指导书的描述：

Catalog Manager负责管理数据库的所有模式信息，包括：

1. 数据库中所有表的定义信息，包括表的名称、表中字段（列）数、主键、定义在该表上的索引。

2. 表中每个字段的定义信息，包括字段类型、是否唯一等。

3. 数据库中所有索引的定义，包括所属表、索引建立在那个字段上等。

Catalog Manager还必需提供访问及操作上述信息的接口，供Interpreter和API模块使用。

| 属性           | 含义                 |
| -------------- | -------------------- |
| catalog_buffer | 表的模式，字典的字典 |

大概是这样的：

```
{
	"table1":{"name", "data_num", "index_num","fmt","column_list","index_list","primary_key"}
	"table2":{"name", "data_num", "index_num","fmt","column_list","index_list","primary_key"}
}
```

| 字典的key   | 含义                                                         | 数据结构 |
| ----------- | ------------------------------------------------------------ | -------- |
| name        | 表的名称，也代表了数据文件和索引文件的位置                   | string   |
| data_num    | 表的数据文件个数，在哪里新建文件                             | int      |
| index_num   | 索引数据文件的个数,在哪里新建文件                            | int      |
| fmt         | 属性的类型                                                   | string   |
| column_list | {属性名字 : is_unique}                                       | 字典     |
| index_list  | {index : address}，address为索引的名称+索引，表示所在树的根地址 | 字典     |
| primary_key | 表的主键                                                     | list     |

| 方法                        | 含义                              |
| --------------------------- | --------------------------------- |
| self.read_catalog()         | 返回catalog_buffer                |
| self.write_catalog(catalog) | 修改后的catalog写回catalog_buffer |

## 2.3 record_manager 类

继承catalog

指导书：

Record Manager负责管理记录表中数据的数据文件。主要功能为实现数据文件的创建与删除（由表的定义与删除引起）、记录的插入、删除与查找操作，并对外提供相应的接口。其中记录的查找操作要求能够支持不带条件的查找和带一个条件的查找（包括等值查找、不等值查找和区间查找）。

数据文件由一个或多个数据块组成，块大小应与缓冲区块大小相同。一个块中包含一条至多条记录，为简单起见，只要求支持定长记录的存储，且不要求支持记录的跨块存储。

| 属性     | 说明           |
| -------- | -------------- |
| max_size | data文件的大小 |



| 方法         | 说明                                                         |
| ------------ | ------------------------------------------------------------ |
| create_data  | 创建表的有关数据的操作                                       |
| drop_data    | table_name，删除表的有关数据文件的所有操作                   |
| insert_data  | table_name, value_list，插入记录操作中有关数据文件的所有操作，返回文件的位置 |
| delete_data  | table_name, conditon，删除记录操作中有关数据文件的所有操作   |
| liner_select | table_name, conditon，顺序查找                               |



## 2.4 index_manager 类

继承catalog

指导书：

Index Manager负责B+树索引的实现，实现B+树的创建和删除（由索引的定义与删除引起）、等值查找、插入键值、删除键值等操作，并对外提供相应的接口。

B+树中节点大小应与缓冲区的块大小相同，B+树的叉数由节点大小与索引键大小计算得到。

| 属性    | 含义                                                       |
| ------- | ---------------------------------------------------------- |
| max_num | 搜索码的最大个数，注意叶子和非叶子节点的搜索码个数是相同的 |

| 方法         | 说明     |
| ------------ | -------- |
| index_select | 查找     |
| insert_index | 插入索引 |
| delete_index | 删除索引 |
| create_tree  | 创建B+树 |
| delete_tree  | 删除B+树 |

## 2.5 interpreter 类

指导书：

Interpreter模块直接与用户交互，主要实现以下功能：

1. 程序流程控制，即“启动并初始化 =>‘接收命令、处理命令、显示命令结果’循环 => 退出”流程。

2. 接收并解释用户输入的命令，生成命令的内部数据结构表示，同时检查命令的语法正确性和语义正确性，对正确的命令调用API层提供的函数执行并显示执行结果，对不正确的命令显示错误信息。

检查语法，这部分支持的语法

| 支持的命令                                               | 备注                        |
| -------------------------------------------------------- | --------------------------- |
| login [user_name] with [password]                        | 暂定                        |
| create table [table_name] [column_list, primary key]     | 见标准T-sql语句             |
| drop table [table_name]                                  | 见标准T-sql语句             |
| insert into [table_name] with value [value_list]         | 仅支持一条语句插入          |
| delete from [table_name] where [condition]               | 见指导书                    |
| select [column_list] from [table_name] where [condition] | 支持嵌套查询，暂定          |
| create index [index] on [table_name]                     | 见标准T-sql语句             |
| delete index [index] on [table_name]                     | 见标准T-sql语句             |
| grant [privilege] on [table_name] to [user_name]         | 见标准T-sql语句，暂时不实现 |
| revoke [privilege] on [table_name] to [user_name]        | 见标准T-sql语句，暂时不实现 |
| execfile [file_name]                                     | 执行文件，暂定              |
| quit                                                     | 退出系统                    |

## 2.6 api 类

指导书：

API模块是整个系统的核心，其主要功能为提供执行SQL语句的接口，供Interpreter层调用。该接口以Interpreter层解释生成的命令内部表示为输入，根据Catalog Manager提供的信息确定执行规则，并调用Record Manager、Index Manager和Catalog Manager提供的相应接口进行执行，最后返回执行结果给Interpreter模块。

提供错误代码，提供执行方法

| 方法            | 含义                 |
| --------------- | -------------------- |
| create_table    |                      |
| drop_table      |                      |
| insert          |                      |
| delete          |                      |
| select          |                      |
| delete_index    |                      |
| create_index    |                      |
| check_user      | 验证用户，暂定       |
| check_privilege | 验证是否有权限，暂定 |
| write_log       | 写入日志，暂定       |
| grant           |                      |
| revoke          |                      |

在传入参数的时候，就先检查语法。

| 属性         | 含义     |
| ------------ | -------- |
| current_user | 当前用户 |



# 3 文件存储

文件存储如下：

```
../db_files
	|--	catalog.json
	|-- privilege.json
	|-- user.json
	|-- log.json
	|--	index
		|-- table_name
			|-- 1.dat
			|-- 2.dat
		|-- table_name2
			|-- 1.dat
			|-- 2.dat
	|-- data
		|-- table_name
			|-- 1.json
			|-- 2.json
```

## 4 数据存储

catalog

```{
{
	"table1":{"name", "data_num", "index_num","fmt","column_list","index_list","primary_key"}
	"table2":{"name", "data_num", "index_num","fmt","column_list","index_list","primary_key"}
}
```

user

```
{
	user_name: password,
	user_name: password
}
```

privilege

```
{
	"user_name":{"table_name":{"wen","ren","is_owner"},"table_name":{"wen","ren","is_owner"}},
	"user_name":{"table_name":{"wen","ren","is_owner"},"table_name":{"wen","ren","is_owner"}},
}
```

log

```
{
	"time":{"user_name", "operation", "result"},
	"time":{"user_name", "operation", "result"},
}
```

index

```
{
	parent:,
	index_list:[],
	address_list:[]
    is_leaf:
}
```

data

```
bool aaaaa aaaa aaaa aaaa
bool bbbbb bbbb bbbb bbbb
```
