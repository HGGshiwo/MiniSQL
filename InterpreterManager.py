from API import Api
from cmd import Cmd
import sys
import time
import os
import re

class InterpreterManager(Cmd , Api):
    def __init__(self):
        Api.__init__(self)
        Cmd.__init__(self)

    # def get_cmd(self):
    #     pass
    def do_execfile(self,args):
        f = open(args)
        text = f.read()
        f.close()
        commands = text.split(';')
        commands = [i.strip().replace('\n','') for i in commands]
        # __initialize__()
        for command in commands:
            if command == '':
                continue
            if command[0] == '#':
                continue
            if command.split(' ')[0] == 'insert':
                # try:
                #     Api.insert(command[6:])
                # except Exception as e:
                #     print(str(e))
                self.do_insert(command[6:])
            elif command.split(' ')[0] == 'select':
                # try:
                #     Api.select(command[6:])
                # except Exception as e:
                #     print(str(e))
                self.do_select(command[6:])
            elif command.split(' ')[0] == 'delete':
                # try:
                #     Api.delete(command[6:])
                # except Exception as e:
                #     print(str(e))
                self.do_delete(command[6:])
            elif command.split(' ')[0] == 'drop':
                # try:
                #     Api.drop(command[4:])
                # except Exception as e:
                #     print(str(e))
                self.do_drop(command[4:])
            elif command.split(' ')[0] == 'create':
                # try:
                #     Api.create(command[6:])
                # except Exception as e:
                #     print(str(e))
                self.do_create(command[6:])
            elif command.split(' ')[0] == 'quit':
                self.do_quit(command[4:])
        # __finalize__()
        pass

    def do_select(self,args):
        try:
            args = args.replace(';','')
            args = re.sub(r' +', ' ', args).strip().replace('\u200b','') #去除多余空格
            lists = args.split(' ')
            if len(lists) < 3 :
                raise Exception('E1')
            if lists[0]!='*' :
                raise Exception('E1')
            if lists[1] != 'from'  :
                raise Exception('E1')
            end_from = re.search('from', args).end()
            if re.search('where', args):
                start_where = re.search('where', args).start()
                end_where = re.search('where', args).end()
                table_name = args[end_from+1:start_where].strip()
                print(table_name)
                condition = args[end_where+1:].strip()
                print(condition)
                condition_list = condition.split(' and ')                
                print(condition_list)
            else:
                table_name = args[end_from+1:].strip()
                print(table_name)
                condition_list = []
            self.select(table_name,condition_list)
            # res=self.select(table_name,condition_list)
            # print(res)
        except Exception as e:
            print(str(e))

    def do_create(self,args):
        try:
            table_info=[]
            args = args.replace(';','')
            args = re.sub(r' +', ' ', args).strip().replace('\u200b','') #去除多余空格
            lists = args.split(' ')
            if len(lists) < 4 :
                raise Exception('E1')
            if lists[0] == 'table' :
                start_on = re.search('table', args).end()
                start = re.search('\(', args).start()
                end = find_last(args,')')
                table_name = args[start_on:start].strip()
                statement = args[start + 1:end].strip()
                if re.search('primary key *\(',statement).end():
                    pass
                else:
                    raise Exception('I3')
                    #raise Exception('E1')
                primary_place = re.search('primary key *\(',statement).end()
                primary_place_end = re.search('\)',statement[primary_place:]).start()
                primary_key = statement[primary_place:][:primary_place_end].strip()
                cols = statement.split(',')
                for cur_column_statement in cols[0:len(lists)-1]:
                    cur_column_statement = cur_column_statement.strip()
                    cur_lists = cur_column_statement.split(' ')
                    is_unique = False
                    type = ''
                    column_name = cur_lists[0]
                    if re.search('unique',concat_list(cur_lists[1:])) or re.search('unique,',concat_list(cur_lists[1:])) or column_name == primary_key:
                        is_unique = True
                    if re.search('char',concat_list(cur_lists[1:])):
                        length_start = re.search('\(',concat_list(cur_lists[1:])).start()+1
                        length_end = re.search('\)', concat_list(cur_lists[1:])).start()
                        length = int(concat_list(cur_lists[1:])[length_start:length_end])
                        for i in length:
                            type = type + 'c' #.................
                    elif re.search('int', concat_list(cur_lists[1:])):
                        type = 'i'
                    elif re.search('float', concat_list(cur_lists[1:])):
                        type = 'f'
                    else:
                        raise Exception('E3')
                    table_info.append(column_name,type,is_unique,-1)
                    print(table_info)
                
                # seed = False
                # for index,__column in enumerate(cur_table.columns):
                #     if __column.column_name == cur_table.primary_key:
                #         cur_table.primary_key = index
                #         seed = True
                #         break
                # if seed == False:
                #     raise Exception('E1')
                self.create_table(table_name,primary_key,table_info)
            elif lists[0] == 'index' :
                #create index index_name on table_name
                if len(lists) < 4 :
                    raise Exception('E1')
                index_name = lists[1]
                if lists[2] != 'on':
                    raise Exception('E1')
                table_name = lists[3]
                table = self.table_list[table_name]
                len_table = (len(table)-2) // 4
                for i in len_table + 1:
                    if(i == len_table) :
                        raise Exception('R1')
                    if(table[(i << 2) + 2] == index_name):
                        if(i == table[0]):
                            raise Exception('I3')
                        if(table[(i << 2) + 4] != True):
                            raise Exception('I2')
                        if(table[(i << 2) + 5] != -1):
                            raise Exception('I1')
                        index = i
                self.create_index(table_name,index)
            else :
                raise Exception('E1')
        except Exception as e:
            print(str(e))

    def do_drop(self,args):
        try:
            args = args.replace(';','')
            args = re.sub(r' +', ' ', args).strip().replace('\u200b','') #去除多余空格
            lists = args.split(' ')
            if len(lists) < 3 :
                raise Exception('E1')
            if lists[0] == 'table' :
                table_name = lists[1]
                self.catalog_list[table_name] = -1
                self.drop_table(table_name)      
            elif lists[0] == 'index' :
                #drop index index_name on table_name
                if len(lists) < 4 :
                    raise Exception('E1')
                index_name = lists[1]
                if lists[2] != 'on':
                    raise Exception('E1')
                table_name = lists[3]
                table = self.table_list[table_name]
                len_table = (len(table)-2) // 4
                for i in len_table + 1:
                    if(i == len_table) :
                        raise Exception('R1')
                    if(table[(i << 2) + 2] == index_name):
                        if(i == table[0]):
                            raise Exception('I3')
                        if(table[(i << 2) + 5] == -1):
                            raise Exception('I4')
                        index = i
                        table[(i << 2) + 5] == -1 #create那边没问题吧
                self.drop_index(table_name,index)
            else :
                raise Exception('E1')
        except Exception as e:
            print(str(e))

    def do_insert(self,args):
        try:
            value_list=[]
            args = args.replace(';','')
            args = re.sub(r' +', ' ', args).strip().replace('\u200b','') 
            lists = args.split(' ')
            print(lists)
            if len(lists) < 4 : 
                raise Exception('E1')
            if lists[0] != 'into'  :
                raise Exception('E1')
            if lists[2] != 'values':
                raise Exception('E1')
            value = args[re.search('\(',args).start()+1:find_last(args,')')]
            values = value.split(',')
            print(values)
            table_name = lists[1]
            cur_table = self.table_list[table_name]
            len_table = (len(cur_table)-2)//4
            if len_table != len(values):
                raise Exception('R3') #value个数不匹配
                #格式不匹配我不会了
            for index in len_table:
                if cur_table[(index << 2)+3] == 'i':
                    item = int(values[index])
                elif cur_table[(index << 2)+3] == 'f':
                    item = float(values[index])
                else:
                    item = re.sub(r'\'', '', values[index]).strip()
                    fmt = cur_table[(index << 2) + 3] #char(255)写法为ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
                    if len(value) > len(fmt) :
                        raise Exception('too long')
                #unqiue检测，开销预计会很大
                if cur_table[(index << 2) + 4]:
                    line =''
                    line = line + cur_table[(index << 2) + 2]
                    line = line + '='
                    line = line + str(item)
                    print(line)
                    # if(self.select(table_name,line)) :
                    #     raise Exception('R5')
                value_list.append(item)
            self.insert(table_name,value_list)
        except Exception as e:
            print(str(e))

    def do_delete(self,args):
        try:
            args = args.replace(';','')
            args = re.sub(r' +', ' ', args).strip().replace('\u200b','') 
            lists = args.split(' ')
            if len(lists) < 2 :
                raise Exception('E1')
            if lists[0] != 'from':
                raise Exception('E1')
            end_from = re.search('from', args).end()
            if re.search('where', args):
                start_where = re.search('where', args).start()
                end_where = re.search('where', args).end()
                table_name = args[end_from+1:start_where].strip()
                # print(table_name)
                condition = args[end_where+1:].strip()
                # print(condition)
                condition_list = condition.split(' and ')                
                # print(condition_list)
            else:
                table_name = args[end_from+1:].strip()
                print(table_name)
                condition_list = []
            self.delete(table_name,condition_list)
            # res=self.delete(table_name,condition_list)
            # print(res)
        except Exception as e:
            print(str(e))

    def do_quit(self,args):
        try:
            self.quit()
            sys.exit()
        except Exception as e:
            print(str(e))

    def emptyline(self):
        pass

    def default(self, line):
        # print('Unrecognized command.\nNo such symbol : %s' % line)
        raise Exception('E1')

def find_last(string,str):
    last_position=-1
    while True:
        position=string.find(str,last_position+1)
        if position==-1:
            return last_position
        last_position=position

def concat_list(lists):
    statement = ''
    for i in lists:
        statement = statement + i
    return statement

 
