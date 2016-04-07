from searcher import utils
import types


def test_iterate_words_returns_generator():
    text = 'there are four parts'
    generator = utils.iterate_words(text)
    assert isinstance(generator, types.GeneratorType)


def test_iterate_words_text_split():
    text = 'there are many parts'
    generator = utils.iterate_words(text)
    target = map(utils.stem_word, ['there', 'are', 'many', 'parts'])
    assert list(generator) == list(target)


def test_iterate_words_handles_dots_commas_dashes():
    text = 'there,are.many-parts'
    generator = utils.iterate_words(text)
    target = map(utils.stem_word, ['there', 'are', 'many', 'parts'])
    assert list(generator) == list(target)


def test_iterate_words_generates_lowercase_words():
    text = 'THeRE Are mANY PaRTS'
    generator = utils.iterate_words(text)
    target = map(utils.stem_word, ['there', 'are', 'many', 'parts'])
    assert list(generator) == list(target)
