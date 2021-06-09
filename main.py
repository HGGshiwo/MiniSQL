from multiprocessing import Pipe, Manager, Process
from API import api
from BufferManager import write_json, read_json, unload_buffer, Page
import os, signal
# user1: insert 100000 tuples in 7.724733352661133

# user1 insert 50000 tuples in 30.69002652168274
# user2 insert 50000 tuples in 31.418816566467285

# user1 insert 50000 tuples in 30.67324471473694
# user2 insert 50000 tuples in 31.44530487060547


def cleaner(share):
    while True:
        # 释放资源
        for process in share.request_pool.keys():
            if share.request_pool[process] not in share.pin_list:
                share.pin_list.append(share.request_pool[process])
                share.request_pool.pop(process)
        pass


def repair_sys():
    catalog_info = dict({})
    for i in range(100):
        catalog_info[i] = []
    write_json('catalog', catalog_info)


if __name__ == "__main__":
    # 初始化
    # repair_sys()
    manager = Manager()
    share = manager.Namespace()
    share.request_pool = manager.dict({})
    share.user_pool = manager.dict({})
    share.pin_list = manager.list()  # 被占用的列表
    share.buffer_pool = manager.dict({})
    empty_page = [-1, -1, -1, -1, -1, -1, True, None, True, True, 0, []]
    for i in range(1024):
        share.buffer_pool[i] = manager.list(empty_page)  # 创建缓存池

    buffer_info = read_json('buffer')
    catalog_info = read_json('catalog')
    user_info = read_json('user')
    privilege_info = read_json('privilege')

    share.buffer_info = manager.dict(buffer_info)
    share.catalog_info = manager.dict(catalog_info)
    for i in catalog_info.keys():
        table = share.catalog_info[i]
        share.catalog_info[i] = manager.list(table)
    share.user_info = manager.dict(user_info)
    share.privilege_info = manager.dict(privilege_info)

    kwargs = {'share': share}
    c = Process(group=None, target=cleaner, name='cleaner', kwargs=kwargs)
    c.start()
    print('successfully init system.')

    while True:
        command = input()
        if command == 'login':
            user = 'test'
            command_receiver, command_sender = Pipe()
            kwargs = {'conn': command_receiver, 'share': share}
            p = Process(group=None, target=api, name=user, kwargs=kwargs)
            p.start()
            share.user_pool[user] = command_sender
            print('successfully login in')
            pass
        elif command == 'create':
            user = 'test'
            if user not in share.user_pool:
                raise RuntimeError('用户' + user + '未登陆，请先登陆.')
            share.user_pool[user].send('create')
        elif command == 'insert':
            user = 'test'
            share.user_pool[user].send('insert')
        elif command == 'quit':
            os.kill(c.pid, signal.SIGTERM)
            for u in share.user_pool.keys():
                share.user_pool[u].send('quit')
            break
        elif command == 'catalog':
            for table in share.catalog_info.keys():
                if len(share.catalog_info[table]) != 0:
                    print(share.catalog_info[table])
        elif command == 'buffer':
            for i in share.buffer_pool.keys():
                if len(share.buffer_pool[i]) != 0:
                    print(share.buffer_pool[i])
        else:
            pass

    catalog_info = dict(share.catalog_info)
    for i in catalog_info.keys():
        catalog_info[i] = list(catalog_info[i])
    user_info = dict(share.user_info)
    privilege_info = dict(share.privilege_info)
    buffer = dict(share.buffer_info)
    write_json('catalog', catalog_info)
    write_json('user', user_info)
    write_json('privilege', privilege_info)
    write_json('buffer', buffer_info)
    for i in range(1024):
        if share.buffer_pool[i][Page.is_delete]:
            continue
        page = list(share.buffer_pool[i])
        unload_buffer(page)
    print('successfully quit system')
