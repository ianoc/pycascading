def go(tuple):
    line = tuple.get(0)
    words = line.split()
    for word in words:
        yield [word, 1]
