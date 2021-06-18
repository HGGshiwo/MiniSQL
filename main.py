import struct
import time
from API import Api
from InterpreterManager import InterpreterManager

if __name__ == "__main__":
    user = Api()
    InterpreterManager.prompt = 'MiniSQL > '
    InterpreterManager().cmdloop()
    pass