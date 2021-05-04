import struct
import os
import json

class buffer_manager(object):
    
    '''
    提供写入和缓存的管理
    '''
    data_buffer = {}
    
    def read_data(self, address, fmt, column_list = [], index_list = []):
        '''
        按照指定格式从文件中读数据
        '''
        if not os.path.exists(address):
            file = open(address, 'a+')
            file.close()

        fmt = fmt + '?'
        if address not in buffer_manager.data_buffer.keys():
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
        
        data_list = buffer_manager.data_buffer[address]      
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
        if address not in buffer_manager.data_buffer.keys():
            self.read_data(address, fmt, list(data.keys()))
        
        buffer_manager.data_buffer[address].append(data)
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
        if address not in buffer_manager.data_buffer.keys():
            file = open(address, 'r')
            buffer_manager.data_buffer[address] = json.load(file)
            file.close()

        return self.data_buffer[address]
    
    def write_index(self, address, node):
        '''
        将一个字典写入文件，字典永远不会超出，因为树会检查，字典是覆盖
        '''
        node_data_buffer = json.dumps(node, indent=4, ensure_ascii=False)
        file = open(address, 'w')
        file.write(node_data_buffer)
        file.close()