from InterpreterManager import InterpreterManager

if __name__ == "__main__":
    print("Hello MiniSQL")
    InterpreterManager.prompt = 'MiniSQL > '
    InterpreterManager().cmdloop()