# 演示流程

## 1.create table

按如下格式创建表，包含整型、长度为10的字符串、浮点数，其中ID和rank属性是unique的

```create table student (ID int unqiue , rank int unique , name char(20) , score float, primary key(ID) );```

## 2.insert

插入1000条数据，示例如下：

```insert into test values (410, 0, Tom, 0.9473110135533653)```

插入结束后展示文件存储，位置为db_files，内容为空，未写入

退出后展示文件存储，内容被写入，为二进制比特数据

## 3.create index
在rank属性创建索引

```create index rank on student```

## 4.select

使用主索引ID、二级索引rank以及非索引属性进行查询，覆盖所有算数比较符，以及无条件情况、多条件情况

```
select * from student 
select * from student where rank = 99
select * from student where rank > 900
select * from student where rank<=99
select * from student where rank <> 99
select * from student where ID = 99
select * from student where name = Leo
select * from student where name = Leo and score < 0.5
```

## 5.drop index
删除在rank上创建的index

```drop index rank on student```



## 6.delete

按指定条件删除

```delete from student where rank < 99```

删除后再用select进行检验

```select * from student where rank <= 99```

全部删除

```delete from student```

全部删除后检验

```select * from student```

## 7.drop table 

```drop table student```

删除后进行检验，兼展示报错功能

```select * from student```

| 错误代码 | 含义                                      |
| -------- | ----------------------------------------- |
| E1       | 错误的指令                                |
| E2       | 缺少参数                                  |
| E3       | 不支持的数据类型                          |
| E4       | 数据长度过长                              |
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
| I4       | 删除时尚未创建索引                        |

## 8.quit

```quit```

退出时会有提示