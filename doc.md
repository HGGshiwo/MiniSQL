# MiniSQL说明文档

## 1 概览

数据库设计的两个原则：1.数据文件存放紧凑 2.查找文件方便

具体实现时候尽量做到：1.逻辑较为简明 2.存储尽量使用已有的数据结构

数据库文件分为两类：数据库管理文件记录数据库模式和用户权限等。

数据库数据文件记录用户存储的数据。

为了简化逻辑，达到第一个原则，数据库数据文件用二进制存储，数据库管理文件用json存储

数据库索引文件用json存储

### 2 MiniSQL类(class MiniSQL)

| 属性名称     | 含义                             | 数据结构                              |
| ------------ | -------------------------------- | ------------------------------------- |
| current_user | 当前的用户名，为了权限检查       | string                                |
| sys_buffer   | 数据库的管理数据                 | 字典的字典，json文件，和sys中内容相同 |
| data_buffer  | 文件名，文件中的数据，方便select | 字典，                                |

#### init方法

加载sys



#### read_data方法

读数据文件



#### wirte\_data方法

写数据文件



#### read_index方法

读索引文件



#### write_index方法

写索引文件



#### create_table方法

检查语法，抛出异常

新建data文件，index文件

更新sys_buffer：新增项和sys_buffer["privilege"]



#### insert方法

检查语法，抛出异常

更新data文件，index文件

更新sys_buffer(如果增加了data或者index文件)

递归调用浪费大量空间，时间复杂度总是树的高度，因此使用循环解决。

​    循环分为以下几步：

​    1.循环到叶子

​    2.循环以下步骤直到不需要分裂

​    3.找到节点中插入的位置

​    4.插入

​    5.如果不需要分裂，退出

​    6.如果需要分裂：

​    叶节点：左孩子增加一个指向右孩子的指针

​    根节点：新建根，更新根，结束

​    非叶节点：分裂出的另一个根的孩子必须修改父亲

​    更新需要插入的值

​    向父节点插入

#### select方法

condion是一个中缀表达式

where column1 > 3 and colmn2 < 4

详见https://blog.csdn.net/qq_40840633/article/details/101060350

仅包含索引的，用索引计算。不然遍历整个data文件



#### drop方法

检查语法

删除文件夹

删除sys_buffer中的表



#### delete方法

检查语法

更新sys_buffer

更新data和index(如果删除的是索引所在的属性)




#### create index方法

更新sys_buffer

更新index



#### delete index 方法

更新sys_buffer

更新index



#### grant方法

更新sys_buffer



#### revoke方法

更新sys_buffer



## 3 数据存储

### 3.1 数据库系统数据(sys)

```
sys={
	"user":[{"user_name":value, "password":value}],
	"privilege":[{"user_name","table_name","wen","ren","is_owner"},],
	"log":[{"time", "user_name", "operation", "result"},],
	"table1":{"name", "data_num", "index_num","fmt","column_list","index_list","primary_key"}
	...
}
```

外字典中自带3个key，这三个键的值为字典的列表：

| user列表中的字典键 | 含义   |
| ------------------ | ------ |
| user_name          | 用户名 |
| password           | 密码   |

| privilege列表中的字典键 | 含义       |
| ----------------------- | ---------- |
| user_name               | 用户名     |
| table_name              | 表名       |
| wen                     | 写权限     |
| ren                     | 读权限     |
| is_owner                | 表的所有者 |

| log列表中的字典键 | 含义     |
| ----------------- | -------- |
| time              | 操作时间 |
| user_name         | 用户名称 |
| operation         | 操作     |
| result            | 错误码   |

其他的是用户创建的表。

内字典中存储的内容有：

| 字典的key   | 含义                                                 | 数据结构 |
| ----------- | ---------------------------------------------------- | -------- |
| name        | 表的名称，也代表了数据文件和索引文件的位置           | string   |
| data_num    | 表的数据文件个数，在哪里新建文件                     | int      |
| index_num   | 索引数据文件的个数,在哪里新建文件                    | int      |
| fmt         | 属性的类型                                           | string   |
| column_list | 属性名字，列名的列表                                 | list     |
| index_list  | 字典，index : address，索引的名称+索引所在树的根地址 | 字典     |
| primary_key | 表的主键                                             | list     |

### 3.2 索引文件存储

使用一个json文件存储

| 属性名      | 含义                           | 数据类型 |
| ----------- | ------------------------------ | -------- |
| address     | 所在数据文件的地址，以及第几条 | 列表     |
| index_value | 索引的值                       | 列表     |
| is_leaf     | 是否是叶子                     | bool     |
| parent      | 父亲的地址                     | string   |

### 3.3 数据文件存储

数据文件（data）：内容是bool类型+数据字典的二进制值

```python
bool a b c #第一条记录
bool c d e #第二条记录，真实存储时不分行
```

数据文件是追加，索引文件是覆盖，不要搞错了



## 4 文件结构

```
../MiniSQL
	|--	sys //不是文件夹
	|--	table1
		|-- data
			|-- 1
			|-- 2
			|-- 3
			|-- ...
		|--	index
			|-- 1
			|-- 2
			|-- 3
	|--	table2
		|-- data
			|-- 1
			|-- 2
			|-- 3
		|-- index
			|-- 1
			|-- 2
			|-..
```

## 5 类型约定

### fmt

python格式字符，支持的类型见下表

| 格式字符 | 含义   |
| :------- | :----- |
| c        | char   |
| s        | string |
| i        | int    |
| ?        | bool   |
| d        | double |

必须在格式字符前注明格式字符的长度。比如5个字符的string表示为：5s

如果表的类型为：char(4), int, int，则fmt = 4s1i1i

#### 错误码

| 错误码                | 含义         |
| :-------------------- | :----------- |
| no_error              | 无错误       |
| table_name_duplicate  | 表名称重复   |
| table_name_not_exist  | 表名称不存在 |
| column_name_duplicate | 列名称重复   |
| column_name_not_exist | 列名称重复   |
| type_not_support      | 不支持的类型 |

## 6 用户界面

| 支持的命令                                               | 备注               |
| -------------------------------------------------------- | ------------------ |
| login [user_name] with [password]                        | 暂定               |
| create table [table_name] [column_list, primary key]     | 见标准T-sql语句    |
| drop table [table_name]                                  | 见标准T-sql语句    |
| insert into [table_name] with value [value_list]         | 仅支持一条语句插入 |
| select [column_list] from [table_name] where [condition] | 支持嵌套查询，暂定 |
| create index [index] on [table_name]                     | 见标准T-sql语句    |
| delete index [index] on [table_name]                     | 见标准T-sql语句    |
| grant [privilege] on [table_name] to [user_name]         | 见标准T-sql语句    |
| revoke [privilege] on [table_name] to [user_name]        | 见标准T-sql语句    |
| execute [file_name]                                      | 执行文件，暂定     |
| quit                                                     | 退出系统           |

MiniSQLTool文件并非一个类

是用户交互界面，主要实现用户语句分解，输出查询结果等

输出的格式：查询的输出格式

时间：操作所花费的时间

结果：是否成功



## 7 权限管理

权限分为三种

| 权限     | 解释                         | 权限转移                                       |
| -------- | ---------------------------- | ---------------------------------------------- |
| is_owner | 表的所有者，具有读写删除权限 | 可以赋予/收回读写权限                          |
| wen      | 写权限                       | 如果有该权限，可以赋予给其他用户，无法收回权限 |
| ren      | 读权限                       | 同上                                           |

root用户是所有表的所有者。

## 8 优点

1.数据文件二进制存储，分结点存储

2.索引文件结点存储

3.自定义文件大小

4.索引的json保存方式

5.操作写入log

6.完善的报错

7.支持中缀表达式查询

8.相对完善的权限管理