import time
from API import Api

if __name__ == "__main__":
    user = Api()
    user.new_table('test', 0, ['index','i',True,-1, 'a','c', False, -1])
    user.print_info()
    last_time = time.time()
    for i in range(1000):
        user.insert('test', [i, 'a'])
    print("insert in "+str(time.time() - last_time))
    # for addr in user.addr_list:
    #     if addr == -1:
    #         continue
    #     user.print_header(addr)
    user.exit()
    pass
