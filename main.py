import struct
import time
from API import Api
from InterpreterManager import InterpreterManager
import random

if __name__ == "__main__":
    user = Api()
    # InterpreterManager.prompt = 'MiniSQL > '
    # InterpreterManager().cmdloop()
    data = ''
    num = 1000
    index = [i for i in range(num)]
    sec_index = [i for i in range(num)]
    random.shuffle(index)
    with open('name.txt', 'r') as file:
        name_list = file.read()
    name_list = name_list.split('\n')

    user.create_table(
        'test', 0, ['index', 'i', True, -1, 'sec', 'i', True, -1, 'name', '20s', False, -1, 'f', 'f', False, -1,])
    for i in range(1):
        name = 'Leo' #random.choice(name_list)
        f = random.random()
        value_list = [index[i], sec_index[i], name, f]
        print(value_list)
        user.insert('test', value_list)
        # for j in range(5):
        #     if user.addr_list.count(j) != 0:
        #         addr = user.addr_list.index(j)
        #         user.print_header(addr)
        #         user.print_record(addr)
    ret = user.select('test', ['name = Leo'])
    print(ret)
    user.create_index('test', 1)
    ret = user.select('test', ['index >= 99'])
    print(ret)
    pass