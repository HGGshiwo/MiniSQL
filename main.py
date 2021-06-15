import struct
import time
from API import Api

if __name__ == "__main__":
    user = Api()
    # user.print_header(0)
    # user.print_record(0)
    # print(" ")
    # user.print_header(1)
    # user.print_record(1)
    # print(" ")
    # user.print_header(2)
    # user.print_record(2)
    # user.load_page(0)
    # user.print_record
    t = time.time()
    user.create_table('test', 0, ['index','i',True, -1, 'a','i', True, -1])
    for i in range(1000):
        user.insert('test', [i, i])
    print('insert 1000 tuples in ' + str(time.time()-t))

    user.drop_table('test')

    t = time.time()
    user.create_index('test', 1)
    print('create index in ' + str(time.time() - t))

    t = time.time()
    ret = user.select('test', ['a = 245'])
    print('select finished in '+str(time.time() - t))
    print(ret)

    user.drop_index('test', 1)
    t = time.time()
    ret = user.select('test', ['a = 245'])
    print('select finished in '+str(time.time() - t))
    print(ret)

    pass
    # user.quit()
    # user.load_page(2)
    # user.print_record(0)
    # user.print_header(0)


    pass
#    user.quit()
    # print('after insert:')
    # user.print_record(0)
    # user.delete_index('test', 0, 1)
    # #  删除字符串首先转为byte
    # print('after delete')
    # user.print_record(0)
    # user.quit()

    # for i in range(100, 200):
    #     user.insert('test', [i, 'a'])
    # print('after insert:')
    # user.print_header(0)
    # user.delete_index('test', 0, 2)
    # #  删除字符串首先转为byte
    # print('after delete')
    # user.print_record(0)
    # user.quit()

    # print(" ")
    # user.print_record(1)
    # print(" ")
    # user.print_record(2)
    # user.print_header(0)
    # print(" ")
    # user.print_record(0)
    # for i in range(200, 250):
    #     user.insert('test', [i, 'a'])
    # print('after insert:')
    # user.print_record(0)
    # user.delete_index('test', 0, 3)
    #  删除字符串首先转为byte
    # print('after delete')
    # user.print_record(0)
    # user.quit()
    pass