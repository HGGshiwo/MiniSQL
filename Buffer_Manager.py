import struct
import os
import json
from enum import IntEnum
from threading  import Lock

class state_flag(IntEnum):
    todo = 1
    doing = 2
    done = 3

class state(object):
    def __init__(self):
        self.state_flag = state_flag.todo

class main_buffer_manager(object):    
    '''
    buffer_manager的主进程
    '''
    is_quit = False
    data_buffer = {}
    queue =  []
    lock = Lock()
    
    def __init__(self):
        while len(main_buffer_manager.queue) != 0 or not main_buffer_manager.is_quit:
            main_buffer_manager.lock.acquire()
            if len(main_buffer_manager.queue) != 0:
                s = main_buffer_manager.queue.pop(0)
                #print(id(s))
                s.state_flag = state_flag.doing
                while s.state_flag != state_flag.done:
                    pass
                main_buffer_manager.lock.release()
            else:
                main_buffer_manager.lock.release()

    def sys_quit(self):
        main_buffer_manager.is_quit = True

class buffer_manager(main_buffer_manager):
    '''
    提供了内存的读写
    '''
    def __init__(self):
        pass
    
    def read_data(self, address, fmt, column_list = [], index_list = []):
        '''
        按照指定格式从文件中读数据
        '''       
        #放入队列
        s = state()
        #print('read_data' + str(id(s)))
        main_buffer_manager.lock.acquire()
        main_buffer_manager.queue.append(s)
        main_buffer_manager.lock.release()
        while(s.state_flag != state_flag.doing):
            pass

        if not os.path.exists(address):
            file = open(address, 'a+')
            file.close()

        fmt = fmt + '?'
        if address not in main_buffer_manager.data_buffer.keys():
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
            main_buffer_manager.data_buffer[address] = data_list
        
        data_list = main_buffer_manager.data_buffer[address]      
        ret = []
        if len(index_list) == 0:
            index_list = list(range(len(data_list)))
            for index in index_list:
                ret.append(data_list[index])

        s.state_flag = state_flag.done
        return ret

    def write_data(self, address, fmt, data):
        '''
        按照指定格式在文件中写数据, 并将文件加入到data_buffer
        address: 文件地址
        fmt: 数据格式
        data: 元组，要求字符格式
        '''

        if address not in main_buffer_manager.data_buffer.keys():
            self.read_data(address, fmt, list(data.keys()))

        # 放入队列
        s = state()
        #print('write_data' + str(id(s)))
        main_buffer_manager.lock.acquire()
        main_buffer_manager.queue.append(s)
        main_buffer_manager.lock.release()
        while (s.state_flag != state_flag.doing):
            pass

        main_buffer_manager.data_buffer[address].append(data)
        data[''] = True
        data = list(data.values())
        for i in range(len(data)):
            data[i] = str.encode(data[i]) if isinstance(data[i], str) else data[i]
        data_buffer = struct.pack(fmt, *data)
        file = open(address, 'ab+')
        file.write(data_buffer)
        file.close()

        s.state_flag = state_flag.done
        return True
    
    def read_index(self, address):
        '''
        从文件中读取一个字典
        '''   
        #放入队列
        s = state()
        #print('read_index' + str(id(s)))
        main_buffer_manager.lock.acquire()
        main_buffer_manager.queue.append(s)
        main_buffer_manager.lock.release()
        while(s.state_flag != state_flag.doing):
            pass

        if address not in main_buffer_manager.data_buffer.keys():
            file = open(address, 'r')
            main_buffer_manager.data_buffer[address] = json.load(file)
            file.close()

        s.state_flag = state_flag.done
        return main_buffer_manager.data_buffer[address]
    
    def write_index(self, address, node):
        '''
        将一个字典写入文件，字典永远不会超出，因为树会检查，字典是覆盖
        '''
        
        #放入队列
        s = state()
        #print('write_index' + str(id(s)))
        main_buffer_manager.lock.acquire()
        main_buffer_manager.queue.append(s)
        main_buffer_manager.lock.release()
        while s.state_flag != state_flag.doing:
            pass
        
        node_data_buffer = json.dumps(node, indent=4, ensure_ascii=False)
        file = open(address, 'w')
        file.write(node_data_buffer)

        s.state_flag = state_flag.done
        file.close()
