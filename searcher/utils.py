import re


def iterate_words(text):
    return (word.group(0).lower() for word in re.finditer(r'(\w+)', text))
