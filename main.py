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
    # user.print_record(0)
    t = time.time()
    user.new_table('test2', 0, ['index','i',True, -1, 'a','c', False, -1])
    for i in range(1000):
        user.insert('test1', [i, 'a'])
    user.quit()
    print('insert 1000 tuples in ' + str(time.time()-t))
    # user.insert('test1', [290, 'a'])
    #


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