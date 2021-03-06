from API import Api
from cmd import Cmd
import sys
import time
import os
import re


class InterpreterManager(Cmd, Api):
    def __init__(self):
        Api.__init__(self)
        Cmd.__init__(self)

    # def get_cmd(self):
    #     pass
    def do_execfile(self, args):
        f = open(args)
        text = f.read()
        f.close()
        commands = text.split(';')
        commands = [i.strip().replace('\n', '') for i in commands]
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
                self.do_quit()
        # __finalize__()
        pass

    def do_select(self, args):
        try:
            t = time.time()
            column_name=[]
            args = args.replace(';', '')
            args = re.sub(r' +', ' ', args).strip().replace('\u200b', '')  # ??????????????????
            lists = args.split(' ')
            if len(lists) < 3:
                raise Exception('E1')
            if lists[0] != '*':
                raise Exception('E1')
            if lists[1] != 'from':
                raise Exception('E1')
            end_from = re.search('from', args).end()
            if re.search('where', args):
                start_where = re.search('where', args).start()
                end_where = re.search('where', args).end()
                table_name = args[end_from + 1:start_where].strip()
                # print(table_name)
                condition = args[end_where + 1:].strip()
                # print(condition)
                condition_list = condition.split(' and ')
                # print(condition_list)
            else:
                table_name = args[end_from + 1:].strip()
                # print(table_name)
                condition_list = []
            # self.select(table_name, condition_list)
            res=self.select(table_name,condition_list)
            table = self.table_list[table_name]
            len_table = (len(table) - 2) // 4
            for i in range(len_table):
                column_name.append(table[(i<<2)+2])
            # print('name',column_name)
            # print(res)
            # print([column_name,res])
            #res qian jiayiduan
            if(res):
                print_record([column_name, res])
            else:
                print('No result.')
            print('select finished in '+str(time.time() - t))
        except Exception as e:
            print(str(e))

    def do_create(self, args):
        try:
            t = time.time()
            table_info = []
            args = args.replace(';', '')
            args = re.sub(r' +', ' ', args).strip().replace('\u200b', '')  # ??????????????????
            lists = args.split(' ')
            if len(lists) < 4:
                raise Exception('E1')
            if lists[0] == 'table':
                start_on = re.search('table', args).end()
                start = re.search('\(', args).start()
                end = find_last(args, ')')
                table_name = args[start_on:start].strip()
                statement = args[start + 1:end].strip()
                if re.search('primary key *\(', statement).end():
                    pass
                else:
                    raise Exception('I3')
                    # raise Exception('E1')
                primary_place = re.search('primary key *\(', statement).end()
                primary_place_end = re.search('\)', statement[primary_place:]).start()
                primary_key = statement[primary_place:][:primary_place_end].strip()
                cols = statement.split(',')
                # print(primary_key)
                # print(cols)
                # print(len(cols))
                for i,cur_column_statement in enumerate(cols[0:len(cols) - 1]):
                    cur_column_statement = cur_column_statement.strip()
                    cur_lists = cur_column_statement.split(' ')
                    is_unique = False
                    column_name = cur_lists[0]
                    if re.search('unique', concat_list(cur_lists[1:])) or re.search('unique,', concat_list(
                            cur_lists[1:])) or column_name == primary_key:
                        is_unique = True
                        if(column_name == primary_key):
                            primary_key_index = i
                    if re.search('char', concat_list(cur_lists[1:])):
                        length_start = re.search('\(', concat_list(cur_lists[1:])).start() + 1
                        length_end = re.search('\)', concat_list(cur_lists[1:])).start()
                        length = concat_list(cur_lists[1:])[length_start:length_end]
                        fmt = length + 's'
                        #1s?????????
                    elif re.search('int', concat_list(cur_lists[1:])):
                        fmt = 'i'
                    elif re.search('float', concat_list(cur_lists[1:])):
                        fmt = 'f'
                    else:
                        raise Exception('E3')
                    table_info.extend([column_name, fmt, is_unique, -1])
                # print(table_info)
                # print(primary_key_index)
                self.create_table(table_name, primary_key_index, table_info)
                print('create table finished in '+str(time.time() - t))
            elif lists[0] == 'index':
                index = None
                # create index index_name on table_name
                if len(lists) < 4:
                    raise Exception('E1')
                index_name = lists[1]
                if lists[2] != 'on':
                    raise Exception('E1')
                table_name = lists[3]
                table = self.table_list[table_name]
                len_table = (len(table) - 2) // 4
                for i in range(len_table+1):#?????????
                    if i == len_table:
                        raise Exception('R1')
                    if table[(i << 2) + 2] == index_name:
                        if i == table[0]:
                            raise Exception('I3')
                        if table[(i << 2) + 4] is not True:
                            raise Exception('I2')
                        if table[(i << 2) + 5] != -1:
                            raise Exception('I1')
                        index = i
                        break
                self.create_index(table_name, index)
                print('create index finished in '+str(time.time() - t))
            else:
                raise Exception('E1')
        except Exception as e:
            print(str(e))

    def do_drop(self, args):
        try:
            t = time.time()
            args = args.replace(';', '')
            args = re.sub(r' +', ' ', args).strip().replace('\u200b', '')  # ??????????????????
            lists = args.split(' ')
            if len(lists) < 2:
                raise Exception('E1')
            if lists[0] == 'table':
                table_name = lists[1]
                self.drop_table(table_name)
                print('successfully drop table ',table_name)
                print('drop table finished in '+str(time.time() - t))
            elif lists[0] == 'index':
                index = None
                # drop index index_name on table_name
                if len(lists) < 4:
                    raise Exception('E1')
                index_name = lists[1]
                if lists[2] != 'on':
                    raise Exception('E1')
                table_name = lists[3]
                table = self.table_list[table_name]
                len_table = (len(table) - 2) // 4
                for i in range(len_table):
                    if i == len_table:
                        raise Exception('R1')
                    if table[(i << 2) + 2] == index_name:
                        if i == table[0]:
                            raise Exception('I3')
                        if table[(i << 2) + 5] == -1:
                            raise Exception('I4')
                        index = i
                        break 
                self.drop_index(table_name, index)
                print('drop index finished in '+str(time.time() - t))
            else:
                raise Exception('E1')
        except Exception as e:
            print(str(e))

    def do_insert(self, args):
        try:
            t = time.time()
            value_list = []
            args = args.replace(';', '')
            args = re.sub(r' +', ' ', args).strip().replace('\u200b', '')
            lists = args.split(' ')
            # print(lists)
            if len(lists) < 4:
                raise Exception('E1')
            if lists[0] != 'into':
                raise Exception('E1')
            if lists[2] != 'values':
                raise Exception('E1')
            value = args[re.search('\(', args).start() + 1:find_last(args, ')')]
            values = value.split(',')
            # print(values)
            table_name = lists[1]
            if self.catalog_list.count(table_name) == 0:
                raise Exception('T2')
            cur_table = self.table_list[table_name]
            len_table = (len(cur_table) - 2) // 4
            if len_table != len(values):
                raise Exception('R3')  # value???????????????
                # ???????????????????????????
            for index in range(len_table):
                if cur_table[(index << 2) + 3] == 'i':
                    item = int(values[index])
                elif cur_table[(index << 2) + 3] == 'f':
                    item = float(values[index])
                else:
                    item = re.sub(r'\'', '', values[index]).strip()
                    fmt = cur_table[(index << 2) + 3]
                    # print(fmt)
                    fmt = re.sub(r's', '', fmt).strip()
                    # fmt = re.sub(r'c', '', fmt).strip()
                    # print(fmt)
                    # print('int(fmt)=',int(fmt))
                    if len(item) > int(fmt):
                        # print(item)
                        raise Exception('T1')
                # unique??????????????????????????????
                # if cur_table[(index << 2) + 4]:
                #     line = ''
                #     line = line + cur_table[(index << 2) + 2]
                #     line = line + '='
                #     line = line + str(item)
                    # print(line)
                    # if(self.select(table_name,line)) :
                    #     raise Exception('R5')
                value_list.append(item)
            self.insert(table_name, value_list)
            print('insert finished in '+str(time.time() - t))
        except Exception as e:
            print(str(e))

    def do_delete(self, args):
        try:
            t = time.time()
            args = args.replace(';', '')
            args = re.sub(r' +', ' ', args).strip().replace('\u200b', '')
            lists = args.split(' ')
            if len(lists) < 2:
                raise Exception('E1')
            if lists[0] != 'from':
                raise Exception('E1')
            end_from = re.search('from', args).end()
            if re.search('where', args):
                start_where = re.search('where', args).start()
                end_where = re.search('where', args).end()
                table_name = args[end_from + 1:start_where].strip()
                # print(table_name)
                condition = args[end_where + 1:].strip()
                # print(condition)
                condition_list = condition.split(' and ')
                # print(condition_list)
            else:
                table_name = args[end_from + 1:].strip()
                # print(table_name)
                condition_list = []
            self.delete(table_name, condition_list)
            print('delete finished in '+str(time.time() - t))
        except Exception as e:
            print(str(e))

    def do_quit(self,args):
        try:
            self.quit()
            sys.exit()
        except Exception as e:
            print(str(e))

    def default(self, line):
        # print('Unrecognized command.\nNo such symbol : %s' % line)
        print('E1')


def find_last(string, s):
    last_position = -1
    while True:
        position = string.find(s, last_position + 1)
        if position == -1:
            return last_position
        last_position = position


def concat_list(lists):
    statement = ''
    for i in lists:
        statement = statement + i
    return statement


def print_record(data_list):
    """
    ?????????????????????
    :param data_list: ????????????????????????????????????
    !!???????????????????????????!!
    :return:
    """
    r_len = len(data_list[0]) #????????????????????????
    len_list = [0] * r_len
    for r in data_list[1]:
        # print(r)
        for j in range(r_len):
            if len('    ' + str(r[j]) + '    ') > len_list[j]:
                len_list[j] = len('    ' + str(r[j]) + '    ')

    # ????????????????????????
    print('\n+', end='')
    for j in range(r_len):
        print('-' * (len_list[j]) + '+', end='')

    # ??????????????????
    print('\n|', end='')
    for j in range(r_len - 1):
        print(str(data_list[0][j]).center(len_list[j]) + '|', end='')
    print(str(data_list[0][r_len - 1]).center(len_list[r_len - 1]) + '|')

    # ?????????????????????
    print('+', end='')
    for i in range(0, r_len - 1):
        print('-' * (len_list[i]) + '+', end='')
    print('-' * (len_list[r_len - 1]) + '+')

    # ????????????
    for i in range(len(data_list[1])):
        print('|', end='')
        for j in range(r_len - 1):
            print(str(data_list[1][i][j]).center(len_list[j]) + '|', end='')
        print(str(data_list[1][i][r_len -1]).center(len_list[r_len - 1]) + '|')

    # ????????????????????????
    print('+', end='')
    for j in range(r_len - 1):
        print('-' * (len_list[j]) + '+', end='')
    print('-' * (len_list[r_len - 1]) + '+')
