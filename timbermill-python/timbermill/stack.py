class Stack:
    def __init__(self):
        self.__items = []

    def is_empty(self):
        return self.__items == []

    def push(self, item):
        return self.__items.append(item)

    def pop(self):
        return self.__items.pop()

    def peek(self):
        return self.__items[len(self.__items)-1]

    def clear(self):
        if not self.is_empty():
            del self.__items[:]
