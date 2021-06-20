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

    for i in range(1000):
        name = random.choice(name_list)
        f = random.random()
        value_list = [index[i], sec_index[i], name, f]
        print(value_list)
        user.insert('test', value_list)
    user.create_index('test', 1)
    ret = user.select('test', ['index=99'])
    print(ret)
    user.drop_index('test', 1)
    user.delete('test', ['sec < 99'])
    ret = user.select('test', ['sec <= 99'])
    print(ret)
    user.delete('test', [])
    ret = user.select('test', [])
    print(ret)
    pass