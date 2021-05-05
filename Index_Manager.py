
import os
import copy
from Catalog_Manager import catalog_manager

class index_manager(catalog_manager):
    '''
    主要是B+树索引的管理
    '''
    def __init__(self):
        catalog_manager.__init__(self)
        self.max_num = 10 #这个要计算过

    def create_tree(self, table_name, index):
        address = 'db_files/index/' + table_name
        if not os.path.isdir(address):
            os.makedirs(address)
        
        catalog_buffer = self.read_catalog()
        catalog_buffer[table_name]["index_num"] += 1 #增加一个节点
        current_table = catalog_buffer[table_name]
        address = address + '/' + str(current_table["index_num"]) + '.json'
        catalog_buffer[table_name]["index_list"][index] = address
        self.write_catalog(catalog_buffer)
         
    def delete_tree(self, table_name):
        pass

    def index_select(self, table_name, condition):
        pass

    def delete_index(self, table_name, index):
        pass
    
    def insert_index(self, table_name, index, index_value, address):
        '''
        将一个数据地址和搜索码插入到对应的索引文件中
        table_name      表名称，用来计算根
        index           索引名称，用来计算根
        index_value     索引值
        address         数据文件所在位置
        '''
        #循环到叶子
        catalog_buffer = self.read_catalog()
        current_table = catalog_buffer[table_name]
        current_address = current_table["index_list"][index] #根的位置
        current_node = None
        if not os.path.exists(current_address):#如果根为空，新建一个节点
            current_node = {}
            current_node["address"] = []
            current_node["index_value"] = []
            current_node["parent"] = current_address
            current_node["is_leaf"] = True
        else: #找到需要插入的节点   
            while True:
                current_node = self.read_index(current_address)
                if current_node["is_leaf"]:
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
        while True:
            #找到节点中需要插入的位置
            i = 0
            while i < len(current_node["index_value"]):
                if current_node["index_value"][i] >= index_value:
                    break
                i += 1
            #插入
            current_node["index_value"].insert(i, index_value)
            current_node["address"].insert(i, address)

            if len(current_node["index_value"]) < self.max_num:
                self.write_index(current_address, current_node)
                break       
            
            #开始分裂，分裂出来的放在前面   
            catalog_buffer[table_name]["index_num"] += 1
            current_table = catalog_buffer[table_name]
            split_node = {}
            split_node["address"] = []
            split_node["address"].extend(copy.copy(current_node["address"][0:self.max_num//2]))              
            split_address = 'db_files/index/' + table_name + '/' + str(current_table["index_num"]) + '.json'
            split_node["index_value"] = [] 
            split_node["index_value"].extend(copy.copy(current_node["index_value"][0:self.max_num//2]))
            split_node["is_leaf"] = current_node["is_leaf"]
            current_node["address"] = current_node["address"][self.max_num//2:self.max_num+1]
            current_node["index_value"] = current_node["index_value"][self.max_num//2:self.max_num+1]
            split_node["parent"] = current_node["parent"]
        
            if split_node["is_leaf"]: #如果是叶子
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
            if split_node["parent"] == right_address:
                catalog_manager.catalog_buffer[table_name]["index_num"] += 1
                current_table = catalog_manager.catalog_buffer[table_name]
                current_address = 'db_files/index/' + table_name + '/' + str(current_table["index_num"]) + '.json'
                current_node = {}
                current_node["parent"] = current_address
                current_node["is_leaf"] = False
                current_node["index_value"] = []
                current_node["address"] = []
                current_node["address"].append(right_address)
                catalog_manager.catalog_buffer[table_name]["index_list"][index] = current_address
                right_node = self.read_index(right_address)
                right_node["parent"] = current_address
                self.write_index(right_address, right_node)
                split_node = self.read_index(address)
                if not split_node["is_leaf"]:
                    split_node["index_value"].pop(-1) #如果不是叶子，还得删掉第一个搜索码
                split_node["parent"] = current_address
                self.write_index(address, split_node)
                self.write_index(current_address, current_node)

        self.write_catalog(catalog_buffer) #调用结束后都需要写入

