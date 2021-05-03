import os
import json
import struct
import copy
import time
import shutil
from enum import IntEnum 

class error(IntEnum):
    no_error = 0
    table_name_duplicate = 1
    table_name_not_exists = 2
    column_name_duplicate = 3
    column_name_not_exists = 4
    type_not_support = 5
    user_not_exist = 6
    password_not_correct = 7

class MiniSQL(object):

    def __init__(self):
        '''
        系统初始化，读取系统文件，读取权限表，确定当前用户
        '''
        self.current_user = None
        self.data_buffer = {}
        self.sys_buffer = {} #这个不是表
        self.n = 5 #100个节点
        self.m = 200 #字节数

        # 开始读取系统文件
        self.sys_buffer = self.read_index('sys')

    def write_log(self, operation, result, user = None):
        data = {}
        user = self.current_user if user == None else user
        data["time"] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        data["user_name"] = user
        data["operation"] = operation
        data["result"] = result
        self.sys_buffer["log"].append(data)
        self.write_index('sys', self.sys_buffer)
    
    def read_data(self, address, fmt, column_list = [], index_list = []):
        '''
        按照指定格式从文件中读数据
        '''
        if not os.path.exists(address):
            file = open(address, 'a+')
            file.close()

        fmt = fmt + '?'
        if address not in self.data_buffer.keys():
            file = open(address, 'rb') 
            size = struct.calcsize(fmt) #一条记录的字节数
            n = int(os.path.getsize(address)/size) #文件中所有记录的数目
            data_list = []
            
            for i in range(n):
                data_buffer = file.read(size)
                udata = struct.unpack(fmt, data_buffer)
                if(udata[-1] == True):#最后一个是bool类型
                    data = {}
                    for j in range(len(column_list)): #对于每一个属性
                        data[column_list[j]] = str(udata[j], encoding="utf-8") if isinstance(udata[j], bytes) else udata[j]
                    data_list.append(data)
            self.data_buffer[address] = data_list
        
        data_list = self.data_buffer[address]      
        ret = []
        if len(index_list) == 0:
            index_list = list(range(len(data_list)))
            for index in index_list:
                ret.append(data_list[index])
        return ret
    
    def write_data(self, address, fmt, data):
        '''
        按照指定格式在文件中写数据, 并将文件加入到data_buffer
        address: 文件地址
        fmt: 数据格式
        data: 元组，要求字符格式
        '''
        file_size = os.path.getsize(address) if os.path.exists(address) else 0

        fmt = fmt + '?'
        size = struct.calcsize(fmt)
        if(file_size + size > self.m):
            return False    #如果插入后文件大小大于4k，则插入失败
        
        if address not in self.data_buffer.keys():
            self.read_data(address, fmt, list(data.keys()))
        
        self.data_buffer[address].append(data)
        data[''] = True
        data = list(data.values())
        for i in range(len(data)):
            data[i] = str.encode(data[i]) if isinstance(data[i], str) else data[i]
        data_buffer = struct.pack(fmt, *data)
        file = open(address, 'ab+')
        file.write(data_buffer)
        file.close()
        return True
    
    def read_index(self, address):
        '''
        从文件中读取一个字典
        '''
        if address not in self.data_buffer.keys():
            file = open(address, 'r')
            self.data_buffer[address] = json.load(file)
            file.close()

        return self.data_buffer[address]
    
    def write_index(self, address, node):
        '''
        将一个字典写入文件，字典永远不会超出，因为树会检查，字典是覆盖
        '''
        node_buffer = json.dumps(node, indent=4, ensure_ascii=False)
        file = open(address, 'w')
        file.write(node_buffer)
        file.close()
        
    def check_user(self, user_name, password):
        '''
        验证用户
        '''
        is_find = False
        for user in self.sys_buffer["user"]:
            if user["user_name"] == user_name:
                if user["password"] == password:
                    is_find = True
                    break
                else:
                    return error.password_not_correct
        if not is_find:
            return error.user_not_exist
        return error.no_error
    
    def reset_sys(self):
        '''
        重置系统文件
        '''
        for file in list(self.sys_buffer.keys()):
            if file in ["user", "privilege", "log"]:
                continue
            if os.path.isfile(file):
                os.remove(file)
            else:
                shutil.rmtree(file)
                    
        self.sys_buffer = {}     
        self.data_buffer = {}
        self.sys_buffer["privilege"] = []
        self.sys_buffer["user"] = []
        self.sys_buffer["log"] = []

        data = {}
        data['user_name'] = 'root'
        data['password'] = 'root'
        self.sys_buffer["user"].append(data)
        self.write_index("sys", self.sys_buffer)
        self.write_log("reset system", error.no_error, "sys")

    def create_table(self, table_name, column_list, primary_key): 
        '''
        create table (name, type)
        更新sys和sys_buffer

        table_name: 表的名称
        column_list: dict{name, type}
        primary_key: 主键
        '''
        fmt = ''
        #开始语法检查
        for name in self.sys_buffer.keys():
            if table_name == name:
                result = error.table_name_duplicate
                self.write_log("create table", result)
                return result
        
        for name, type in column_list.items():
            appreance = []
            if name in appreance:
                result = error.column_name_duplicate
                self.write_log("create table", result)
                return result
            else:
                appreance.append(name)
            
            if(type[-1] not in ['i', 'c', 's', '?']):
                result = error.type_not_support
                self.write_log("create table", result)
                return result
            
            fmt = fmt + type

        if(primary_key == None):
            primary_key = column_list.keys[0]
        
        #开始更新sys_buffer[table_name]
        table = {}
        table['name'] = table_name
        table['data_num'] = 1
        table['index_num'] = 1
        table['fmt'] = fmt
        table['column_list'] = list(column_list.keys())
        table['index_list'] = {primary_key: table_name + "/index/1"}
        table['primary_key'] = primary_key
        self.sys_buffer[table_name] = table
        
        #开始更新sys_buffer["privilege"]
        privilege = {}
        privilege['user_name'] = self.current_user
        privilege['table_name'] = table_name
        privilege['wen'] = True
        privilege['ren'] = True
        privilege['is_owner'] = True
        self.sys_buffer["privilege"].append(privilege)
        self.write_index('sys', self.sys_buffer) 
        
        #新建data文件
        address = table_name + '/data/'
        os.makedirs(address)   
        
        #新建index文件
        address = table_name + '/index/'
        os.makedirs(address)
        
        #写入log文件
        result = error.no_error
        self.write_log("create table", result)
        return result
    
    def insert_index(self, table_name, index_value, index, address):
        '''
        将一个数据地址和搜索码插入到对应的索引文件中
        table_name      表名称，用来计算根
        index           索引名称，用来计算根
        index_value     索引值
        address         数据文件所在位置
        '''
        #循环到叶子
        current_table = self.sys_buffer[table_name]
        current_address = current_table["index_list"][index] #根的位置
        current_node = None
        if(not os.path.exists(current_address)):#如果根为空，新建一个节点
            current_node = {}
            current_node["address"] = []
            current_node["index_value"] = []
            current_node["parent"] = current_address
            current_node["is_leaf"] = True
        else: #找到需要插入的节点   
            while True:
                current_node = self.read_index(current_address)
                if current_node["is_leaf"] == True:
                    break
                i = 0
                while i < len(current_node["index_value"]) and index_value > current_node["index_value"][i]:
                    i += 1
                current_address = current_node["address"][i]  
        '''
        进行迭代的值
        current_node    当前准备插入的节点
        current_address 当前节点的地址
        index_value     插入当前节点的搜索码的值
        address         需要插入当前节点的指针
        split_node      当前节点分裂出的节点
        split_address   分裂出的节点的地址

        n个搜索码，n+1个指针，1个指向后一个文件的指针。分裂。 
        current_table 是为了查表方便
        必须先更新sys_buffer，再更新current_table，进行查表           
        '''
        #开始插入
        while(True):
            #找到节点中需要插入的位置
            i = 0
            while i < len(current_node["index_value"]):
                if current_node["index_value"][i] >= index_value:
                    break
                i += 1
            #插入
            current_node["index_value"].insert(i, index_value)
            current_node["address"].insert(i, address)

            if len(current_node["index_value"]) < self.n:
                self.write_index(current_address, current_node)
                break       
            
            #开始分裂，分裂出来的放在前面   
            self.sys_buffer[table_name]["index_num"] += 1
            current_table = self.sys_buffer[table_name]
            split_node = {}
            split_node["address"] = []
            split_node["address"].extend(copy.copy(current_node["address"][0:self.n//2]))              
            split_address = table_name + "/index/" + str(current_table["index_num"])
            split_node["index_value"] = [] 
            split_node["index_value"].extend(copy.copy(current_node["index_value"][0:self.n//2]))
            split_node["is_leaf"] = current_node["is_leaf"]
            current_node["address"] = current_node["address"][self.n//2:self.n+1]
            current_node["index_value"] = current_node["index_value"][self.n//2:self.n+1]
            split_node["parent"] = current_node["parent"]
        
            if(split_node["is_leaf"]): #如果是叶子
                split_node["address"].append(current_address)
            else:
                for child_address in split_node["address"]:
                    child = self.read_index(child_address)
                    child["parent"] = split_address
                    self.write_index(child_address, child)
            
            self.write_index(current_address, current_node)
            self.write_index(split_address, split_node) 
            
            #更新迭代变量
            index_value = split_node["index_value"][-1]
            address = split_address
            right_address = current_address
            current_address = current_node["parent"]
            current_node = self.read_index(current_address)
            
            #如果是根的分裂
            if(split_node["parent"] == right_address): 
                self.sys_buffer[table_name]["index_num"] += 1
                current_table = self.sys_buffer[table_name]
                current_address = table_name + '/index/' + str(current_table["index_num"])
                current_node = {}
                current_node["parent"] = current_address
                current_node["is_leaf"] = False
                current_node["index_value"] = []
                current_node["address"] = []
                current_node["address"].append(right_address)
                self.sys_buffer[table_name]["index_list"][index] = current_address
                right_node = self.read_index(right_address)
                right_node["parent"] = current_address
                self.write_index(right_address, right_node)
                split_node = self.read_index(address)
                if not split_node["is_leaf"]:
                    split_node["index_value"].pop(-1) #如果不是叶子，还得删掉第一个搜索码
                split_node["parent"] = current_address
                self.write_index(address, split_node)
                self.write_index(current_address, current_node)

        self.write_index("sys", self.sys_buffer) #调用结束后都需要写入
        result = error.no_error
        self.write_log("insert", result)
        return result
    
    def insert(self, table_name, value_list):
        '''
        在data文件中插入一条数据
        在index文件写入一条数据
        '''
        result = None
        if table_name not in self.sys_buffer.keys():
            result = error.table_name_not_exists
            self.write_log("insert", result)
            return result  

        # 查系统表
        current_table = self.sys_buffer[table_name]
        fmt = current_table["fmt"]
        column_list = current_table["column_list"]
        data_address = table_name + "/data/" + str(current_table["data_num"])
        
        # 更新data文件
        data = {}
        for i, column in enumerate(column_list):
            #这里需要做一个类型检查
            data[column] = value_list[i]
        e = self.write_data(data_address, fmt, data)
        
        #如果data文件已满
        if e == False:
            self.sys_buffer[table_name]["data_num"] += 1
            current_table = self.sys_buffer[table_name]
            data_address = table_name + "/data/" + str(current_table["data_num"])
            self.write_data(data_address, fmt, data)

        size = struct.calcsize(fmt) #一条记录的字节数
        offset = int(os.path.getsize(data_address)/size) #文件中所有记录的数目
        
        for index in list(current_table["index_list"].keys()): 
            data_address = data_address + ":" + str(offset)
            index_value = data[index]
            result = self.insert_index(table_name, index_value, index, data_address)
            if result != error.no_error:
                return result
        return result

    def delete_index(self, table_name, index_value, index):
        '''
        删除索引
        table_name      用来计算根
        index_value     索引的值，搜索码的值
        index           索引名称，用来计算根
        '''

    def select_index(self, table_name, condition):
        '''
        返回的是:
        index指针的位置：index节点的地址 + 节点的第几个指针(如果存在index)
        或者是未找到的error
        '''
        pass

    def select(self, table_name, condition):
        '''
        返回的是字典的列表
        '''
    
    def drop_table(self, table_name):
        pass

    def drop_column(self, table_name, column):
        '''
        可选
        '''
        pass

    def delete(self, table_name, condition):
        '''
        删除data：根据select_index，将对应的记录enable=0
        删除index：根据select_index，将节点的指针删除，然后循环删除
        整个过程和插入差不多
        '''
        pass

    def create_index(self, table_name, index):
        '''
        在指定表创建一个索引
        '''
        pass

    def grant(self, user_name, table_name, privilege):
        pass

    def revoke(self, user_name, table_name, privilege):
        pass 

#test:
root = MiniSQL()
root.reset_sys()
column_list = {'a':'1s', 'b':'1s'}
e = root.create_table('master', column_list, 'a')
# print(e)
for i in range(50):
    value_list = []
    value_list.append(str(i))
    value_list.append('a')
    e = root.insert('master', value_list)
    print(e)
# print(root.sys_buffer)
pass