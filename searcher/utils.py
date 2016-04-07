import re
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
