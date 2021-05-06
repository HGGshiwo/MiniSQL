import struct
import os
import json
from Thread_Manager import thread_manager, request, state, priority

class buffer_manager(thread_manager):
    """
    提供了内存的读写
    """
    data_buffer = {}
    max_num = 32

    def __init__(self):
        pass

    def load_buffer(self, address, node=None, data=None, fmt=None):
        """
        不加锁将数据或者节点从文件或者节点加载入缓存
        """
        if node is not None:
            buffer = node  # 从节点加载入缓存
        elif data is not None:
            buffer = data  # 从数据加载入缓存
        elif fmt is not None:  # 从数据文件加载入缓存
            fmt = fmt + '?'
            file = open(address, 'rb')
            size = struct.calcsize(fmt)  # 一条记录的字节数
            n = os.path.getsize(address) // size  # 文件中所有记录的数目
            data_list = []

            for i in range(n):
                data_buffer = file.read(size)  # 猜测read后指针会自己移动
                udata = struct.unpack(fmt, data_buffer)
                if udata[-1]:  # 最后一个是bool类型
                    data = {}
                    for j in range(len(udata)-1):  # 对于每一个属性.str转为bytes
                        data[j] = str(udata[j], encoding="utf-8") \
                            if isinstance(udata[j], bytes) else udata[j]
                    data_list.append(data)
            buffer = data_list
        else:  # 从节点文件加载入缓存
            file = open(address, 'r')
            buffer = json.load(file)
            file.close()

        if len(buffer_manager.data_buffer) == buffer_manager.max_num:
            # 写入时如果已满，则删除缓存
            min_base = None
            min_times = int('inf')
            for key in list(buffer_manager.data_buffer.keys()):
                if buffer_manager.data_buffer[key]['times'] < min_times:
                    min_times = buffer_manager.data_buffer[key]['times']
                    min_base = key
            buffer_manager.data_buffer.pop(min_base)
            buffer_manager.data_buffer[address] = {'times': 1, 'data': buffer}
        else:
            if address not in buffer_manager.data_buffer.keys():
                buffer_manager.data_buffer[address] = {}
                buffer_manager.data_buffer[address]["times"] = 1
            buffer_manager.data_buffer[address]['data'] = buffer
            buffer_manager.data_buffer[address]['times'] += 1

    def read_data(self, address, fmt, column_list):
        """
        按照指定格式从文件中读数据
        column_list     列表，是列名的列表
        address         字典，是{'base','offset'}
        如果未指定offset，就是读取所有的address
        """
        if not os.path.exists(address):
            file = open(address, 'a+')
            file.close()

        base = address["base"]
        offset = address["offset"] if "offset" in address.keys() else None

        # 申请读取
        req = request(base, priority.read)
        thread_manager.lock.acquire()
        thread_manager.wait_list.append(req)
        thread_manager.lock.release()
        while req.state != state.doing:
            pass

        if base in buffer_manager.data_buffer.keys():
            # 如果命中，直接读取
            buffer_manager.data_buffer[base]['times'] += 1  # 不加锁是因为没人在读data时读这个
        else:
            # 如果没有被命中，申请加载缓存
            req = request(base, priority.write)
            thread_manager.lock.acquire()
            thread_manager.wait_list.append(req)
            thread_manager.lock.release()
            while req.state != state.doing:
                pass
            self.load_buffer(fmt, base, column_list)

        ret = buffer_manager.data_buffer[base]['data'] if offset is None \
            else buffer_manager.data_buffer[base]['data'][offset]
        req.state = state.done
        return ret

    def write_data(self, address, fmt, data):
        """
        按照指定格式在文件中增加数据, 并将文件加入到data_buffer
        address: 字典
        fmt: 数据格式
        data: 列表
        """
        base = address["base"]
        offset = address["offset"] if "offset" in address.keys() else None

        # 放入队列
        req = request(base, priority.write)
        thread_manager.lock.acquire()
        thread_manager.wait_list.append(req)
        thread_manager.lock.release()
        while req.state != state.doing:
            pass

        # 修改文件
        data.append(True)
        for i in range(len(data)):
            data[i] = str.encode(data[i]) if isinstance(data[i], str) else data[i]
        data_buffer = struct.pack(fmt, *data)
        file = open(base, 'ab+')
        file.write(data_buffer)
        file.close()

        # 修改缓存
        if base not in buffer_manager.data_buffer.keys():
            # 如果没有直接命中,加载缓存
            self.load_buffer(address=base, fmt=fmt)
            data.pop(-1)
        buffer_manager.data_buffer[base]['data'].append(data)
        req.state = state.done
        return True
    
    def read_index(self, address):
        """
        从文件中读取一个字典
        """
        # 放入队列
        req = request(address, priority.read)
        thread_manager.lock.acquire()
        thread_manager.wait_list.append(req)
        thread_manager.lock.release()
        while req.state != state.doing:
            pass

        if address not in buffer_manager.data_buffer.keys():
            self.load_buffer(address)

        ret = buffer_manager.data_buffer[address]["data"]
        req.state = state.done
        return ret
    
    def write_index(self, address, node):
        """
        将一个字典写入文件，字典永远不会超出，因为树会检查，字典是覆盖
        """
        # 放入队列
        req = request(address, priority.write)
        thread_manager.lock.acquire()
        thread_manager.wait_list.append(req)
        thread_manager.lock.release()
        while req.state != state.doing:
            pass

        data_buffer = json.dumps(node, indent=4, ensure_ascii=False)
        file = open(address, 'w')
        file.write(data_buffer)
        file.close()
        self.load_buffer(address, node=node)

        req.state = state.done
