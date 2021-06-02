from API import api
from BufferManager import write_json, read_json, unload_buffer, Page

# user1: insert 100000 tuples in 7.724733352661133

# user1 insert 50000 tuples in 30.69002652168274
# user2 insert 50000 tuples in 31.418816566467285

# user1 insert 50000 tuples in 30.67324471473694
# user2 insert 50000 tuples in 31.44530487060547


class share:
    pass


# def cleaner(share):
#     while True:
#         # 释放资源
#         for process in share.request_pool.keys():
#             if share.request_pool[process] not in share.pin_list:
#                 share.pin_list.append(share.request_pool[process])
#                 share.request_pool.pop(process)
#         pass


def repair_sys():
    catalog_info = dict({})
    for i in range(100):
        catalog_info[i] = []
    write_json('catalog', catalog_info)


if __name__ == "__main__":
    # 初始化
    # repair_sys()
    share.request_pool = {}
    share.user_pool = dict({})
    share.pin_list = list()  # 被占用的列表
    share.buffer_pool = dict({})
    empty_page = [-1, -1, -1, -1, -1, -1, True, None, True, True, 0, []]
    for i in range(1024):
        share.buffer_pool[i] = list(empty_page)  # 创建缓存池

    buffer_info = read_json('buffer')
    catalog_info = read_json('catalog')
    user_info = read_json('user')
    privilege_info = read_json('privilege')

    share.buffer_info = dict(buffer_info)
    share.catalog_info = dict(catalog_info)
    for i in catalog_info.keys():
        table = share.catalog_info[i]
        share.catalog_info[i] = list(table)
    share.user_info = dict(user_info)
    share.privilege_info = dict(privilege_info)

    print('successfully init system.')

    api(share)

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
