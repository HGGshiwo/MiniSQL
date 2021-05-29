from multiprocessing import Pipe, Manager, Process
from API import api
from BufferManager import write_json, read_json, unload_buffer
# def test1():
#     """
#     测试点
#     1.建表
#     2.插入少量顺序记录
#     3.记录写入文件
#     4.文件读取
#     """
#     pass
#     a = Api('user1')
#     name_list = ['index', 'a']
#     unique = [True, False]
#     column_list = {}
#     for i in range(len(name_list)):
#         column = Column(unique[i], i)
#         column_list[name_list[i]] = column
#
#     last_time = time.time()
#     fmt_list = Fmt_List(('1i','1s'))
#
#     a.create_table('test', fmt_list, column_list, 'index')
#     print("create table in " + str(time.time()-last_time))
#
#     for i in range(100):
#         last_time = time.time()
#         value_list = [i, 'a']
#         e = a.insert('test', value_list)
#         print('user1:insert ' + str(i) + ' in ' + str(time.time()-last_time))
#     pool = Buffer_Manager.buffer_pool
#     a.unload_buffer(0)
#     pool = Buffer_Manager.buffer_pool
#     page = a.read_buffer(0)
#     pass
# def test2():
#     """
#     测试点
#     1.测试分裂叶子节点
#     2.测试分裂非叶子根节点
#     """
#     pass
#     a = Api('user1')
#     a.max_size = 200
#     name_list = ['index', 'a']
#     unique = [True, False]
#     column_list = {}
#     for i in range(len(name_list)):
#         column = Column(unique[i], i)
#         column_list[name_list[i]] = column
#
#     last_time = time.time()
#     fmt_list = Fmt_List(('1i','1s'))
#
#     a.create_table('test2', fmt_list, column_list, 'index')
#     print("create table in " + str(time.time()-last_time))
#
#     last_time = time.time()
#     for i in range(100000):
#         value_list = [i, 'a']
#         e = a.insert('test', value_list)
#     print('user1:insert ' + '100000 tuples' + ' in ' + str(time.time()-last_time))
#
#     # user1: insert 100000 tuples in 7.724733352661133
#     pass


# def test3_1():
#     """
#     测试点：
#     两个用户插入50000组数据
#     """
#
#     a = Api('user2')
#     for i in range(100000//2,100000):
#         value_list = [i, 'a']
#         e = a.insert('test3_3', value_list)
#     print("user2 insert 50000 tuples in " + str(time.time() - last_time))
#
# def test3_2():
#     a = Api('user3')
#     for i in range(100000//2):
#         value_list = [i, 'a']
#         e = a.insert('test3_3', value_list)
#     print("user1 insert 50000 tuples in " + str(time.time() - last_time))

# user1 insert 50000 tuples in 30.69002652168274
# user2 insert 50000 tuples in 31.418816566467285

# user1 insert 50000 tuples in 30.67324471473694
# user2 insert 50000 tuples in 31.44530487060547


def cleaner(receiver, share):
    while True:
        if receiver.recv == 'quit':
            receiver.close()
            break
        # 释放资源
        for process in share.request_pool.keys():
            if share.request_pool[process] not in share.pin_list:
                share.pin_list.append(share.request_pool[process])
                share.request_pool.pop(process)
        pass


if __name__ == "__main__":
    # 初始化
    manager = Manager()
    share = manager.Namespace()
    share.request_pool = manager.dict({})
    share.user_pool = manager.dict({})
    share.pin_list = manager.list()  # 被占用的列表
    share.buffer_pool = manager.dict({})

    buffer_info = read_json('buffer')
    catalog_info = read_json('catalog')
    user_info = read_json('user')
    privilege_info = read_json('privilege')

    share.buffer_info = manager.dict(buffer_info)
    share.catalog_info = manager.dict(catalog_info)
    share.user_info = manager.dict(user_info)
    share.privilege_info = manager.dict(privilege_info)
    clean_receiver, clean_sender = Pipe()
    kwargs = {'receiver': clean_receiver, 'share': share}
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
            share.user_pool[user].send('create')
            for u in share.user_pool.keys():
                if u == user:
                    continue
                share.user_pool[u].send('None')
        elif command == 'insert':
            user = 'test'
            share.user_pool[user].send('insert')
            for u in share.user_pool.keys():
                if u == user:
                    continue
                share.user_pool[u].send('None')
        elif command == 'quit':
            clean_sender.send('quit')
            clean_sender.close()
            clean_receiver.close()
            for u in share.user_pool.keys():
                share.user_pool[u].send('quit')
            break
        else:
            for user in share.user_pool.keys():
                share.user_pool[user].send('None')
            clean_sender.send('None')
            pass

    catalog_info = dict(share.catalog_info)
    user_info = dict(share.user_info)
    privilege_info = dict(share.privilege_info)
    buffer = dict(share.buffer_info)
    write_json('catalog', catalog_info)
    write_json('user', user_info)
    write_json('privilege', privilege_info)
    write_json('buffer', buffer_info)
    key_list = list(share.buffer_pool.keys())
    for page_no in key_list:
        page = share.buffer_pool[page_no]
        unload_buffer(page)
        print(page['user_record'])
    print('successfully quit system')
