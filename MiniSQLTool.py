from MiniSQL import MiniSQL
from MiniSQL import error
import re

if __name__ == "__main__":
    start_message = '''
MiniSQL[2019/4/30 powered by Ser]

    '''
    print(start_message)

    #开始登陆
    while True:
        command = input("MiniSQL> ")
        command = re.sub('\s','',command)
        
        pa = re.compile(r'(?<=login)(.*)(?=with)')
        user = pa.search(command)
        if(user == None):
            print("MiniSQL> 不支持的用户名。")
            continue
        user = user.group()

        pa = re.compile(r'(?<=with)(.*)')
        password = pa.search(command)
        if(password == None):
            print("MiniSQL> 不支持的密码。")
            continue
        password = password.group()
        
        a = MiniSQL()
        e = a.check_user(user, password)
        
        if(e == error.user_not_exist):
            print("MiniSQL> 用户不存在。")
        elif(e == error.password_not_correct):
            print("MiniSQL> 密码错误。")
        else:
            print("MiniSQL/" + user + " > 登陆成功。欢迎您。")
            break
    
    #开始等待操作
    pa = re.compile(r'?<=(\s*))(\S*)')
    