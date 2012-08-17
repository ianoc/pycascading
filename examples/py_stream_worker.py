def go(input):
    words = input.split()
    for word in words:
        yield [word, 1]
