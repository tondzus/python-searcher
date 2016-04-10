import os
import types
from searcher import utils


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


def test_document_count(tmpdir):
    tmp = tmpdir.mkdir('test_doc_count')
    tmp.join('one').write('one')
    tmp.join('two').write('two')
    tmp.join('three').write('three')
    assert utils.document_count(str(tmp)) == 3


def test_document_count_nested(tmpdir):
    tmp = tmpdir.mkdir('test_doc_count')
    nested_tmp = tmp.mkdir('nested')
    tmp.join('one').write('one')
    nested_tmp.join('two').write('two')
    nested_tmp.join('three').write('three')
    assert utils.document_count(str(tmp)) == 3


def test_document_iterator(tmpdir):
    tmp = tmpdir.mkdir('test_doc_count')
    tmp.join('one').write('one')
    tmp.join('two').write('two')
    tmp.join('three').write('three')

    paths = list(utils.files_iterator(str(tmp)))
    assert os.path.join(str(tmp), 'one') in paths
    assert os.path.join(str(tmp), 'two') in paths
    assert os.path.join(str(tmp), 'three') in paths
    assert len(paths) == 3


def test_document_iterator_nested(tmpdir):
    tmp = tmpdir.mkdir('test_doc_count')
    nested_tmp = tmp.mkdir('nested')
    tmp.join('one').write('one')
    nested_tmp.join('two').write('two')
    nested_tmp.join('three').write('three')

    paths = list(utils.files_iterator(str(tmp)))
    assert os.path.join(str(tmp), 'one') in paths
    assert os.path.join(str(nested_tmp), 'two') in paths
    assert os.path.join(str(nested_tmp), 'three') in paths
    assert len(paths) == 3
