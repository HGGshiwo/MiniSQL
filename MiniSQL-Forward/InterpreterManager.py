from API import Api


class InterpreterManager(Api):
    def __init__(self):
        Api.__init__(self)
        while True:
            cmd = input()
            # do something here
        pass

    def get_command(self, cmd):
        pass
