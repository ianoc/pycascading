def go(tuple):
    for entry in tuple:
        words = str(entry).split()
        for word in words:
            yield [word, 1]
