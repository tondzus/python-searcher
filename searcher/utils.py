import re
import os
from itertools import chain
try:
    from nltk import PorterStemmer
except ImportError:
    def stem_word(word):
        return word
else:
    _ps = PorterStemmer()
    def stem_word(word):
        return _ps.stem_word(word)


def iterate_words(text):
    return (stem_word(word.group(0).lower())
            for word in re.finditer(r'(\w+)', text))


def files_iterator(path):
    def mass_join(root, files):
        return [os.path.join(root, fn) for fn in files]

    return chain.from_iterable(mass_join(root, files)
                               for root, _, files in os.walk(path))


def document_count(path):
    return len(list(files_iterator(path)))

