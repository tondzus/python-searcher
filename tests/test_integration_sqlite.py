import os
import csv
import pytest
import sqlite3
import configparser
from searcher.sqlite import SQLiteController


class Args:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def csv_import(csv_path, dbpath):
    q = 'INSERT INTO indexes (document_id, word, rank) VALUES (?, ?, ?)'
    db = sqlite3.connect(dbpath)
    with open(csv_path) as fp:
        reader = csv.reader(fp)
        curr = db.cursor()
        for _, doc_id, word, rank in reader:
            curr.execute(q, (doc_id, word, rank))
        db.commit()
        db.close()

@pytest.fixture(scope='module')
def config(request):
    cnf = configparser.ConfigParser()
    cnf['default'] = {'datastore': 'sqlite3',
                      'query_limit': 10}
    res = os.path.join(str(request.config.rootdir), 'tests', 'resources')
    cnf['sqlite3'] = {'documents': '{}/doc.db'.format(res),
                      'document_batch_store_size': 100,
                      'indexes': '{}/idx.db'.format(res)}
    return cnf


@pytest.fixture(scope='function', autouse=True)
def clean_resources(request, config):
    docdb = config.get('sqlite3', 'documents')
    idxdb = config.get('sqlite3', 'indexes')
    def fin():
        if os.path.isfile(docdb):
            os.remove(docdb)
        if os.path.isfile(idxdb):
            os.remove(idxdb)
    request.addfinalizer(fin)


@pytest.fixture(scope='function')
def document_root(request):
    res = os.path.join(str(request.config.rootdir), 'tests', 'resources')
    return os.path.join(res, 'documents')


@pytest.fixture(scope='function')
def controller(config):
    return SQLiteController(config)


@pytest.fixture(scope='function')
def controller_init(controller):
    controller.init(Args(force=True))
    return controller


@pytest.fixture(scope='function')
def controller_docs(controller_init, document_root):
    controller_init.register(Args(root=document_root))
    return controller_init


@pytest.fixture(scope='function')
def controller_idx(config, controller_docs, tmp_csv_buffer):
    controller_docs.index_store.csv_buffer = tmp_csv_buffer
    controller_docs.index(Args())
    csv_import(tmp_csv_buffer, config.get('sqlite3', 'indexes'))
    return controller_docs


@pytest.fixture(scope='function')
def tmp_csv_buffer(request):
    res = os.path.join(str(request.config.rootdir), 'tests', 'resources')
    csv_path = os.path.join(res, 'tmp.csv')
    def fin():
        if os.path.isfile(csv_path):
            os.remove(csv_path)
    request.addfinalizer(fin)
    return csv_path


def test_database_init(config, controller):
    args = Args(force=False)
    controller.init(args)
    assert os.path.isfile(config.get('sqlite3', 'documents'))


def test_database_init_db_exist(config, controller):
    args = Args(force=False)

    docdb = config.get('sqlite3', 'documents')
    with open(docdb, 'w') as fp:
        fp.write('rubish')
    created = os.stat(docdb).st_mtime

    controller.init(args)
    modified = os.stat(docdb).st_mtime
    assert created == modified


def test_database_force_init(config, controller):
    args = Args(force=True)

    docdb = config.get('sqlite3', 'documents')
    with open(docdb, 'w') as fp:
        fp.write('rubish')
    created = os.stat(docdb).st_mtime

    controller.init(args)
    modified = os.stat(docdb).st_mtime
    assert created < modified


def test_document_register(config, controller_init, document_root):
    controller_init.register(Args(root=document_root))
    docdb = config.get('sqlite3', 'documents')
    conn = sqlite3.connect(docdb)
    doc_count = conn.execute('SELECT COUNT(*) FROM documents').fetchall()[0]
    assert doc_count[0] == len(os.listdir(document_root))


def test_document_index(config, controller_docs, document_root,
                        tmp_csv_buffer):
    controller_docs.index_store.csv_buffer = tmp_csv_buffer
    controller_docs.index(Args())
    csv_import(tmp_csv_buffer, config.get('sqlite3', 'indexes'))
    idxdb = sqlite3.connect(config.get('sqlite3', 'indexes'))
    result = idxdb.execute('SELECT DISTINCT(word) FROM indexes').fetchall()
    index_words = {r[0] for r in result}
    assert len(index_words) > 0


def test_document_show_content(config, controller_docs, capsys):
    conn = sqlite3.connect(config.get('sqlite3', 'documents'))
    document = conn.execute('SELECT * FROM documents LIMIT 1').fetchall()[0]
    controller_docs.show(Args(document_id=[document[0]], preview=False))
    content, _ = capsys.readouterr()
    assert content[:-1] == document[1]


def test_document_show_preview(config, controller_docs, capsys):
    conn = sqlite3.connect(config.get('sqlite3', 'documents'))
    document = conn.execute('SELECT * FROM documents LIMIT 1').fetchall()[0]
    controller_docs.show(Args(document_id=[document[0]], preview=True))
    preview, _ = capsys.readouterr()
    assert preview in document[1]


def test_query(config, controller_idx, capsys):
    conn = sqlite3.connect(config.get('sqlite3', 'indexes'))
    result = conn.execute('SELECT word FROM indexes LIMIT 1').fetchall()[0]
    word = result[0]
    args = Args(measure=False, query_string=word, preview=False)
    controller_idx.query(args)
    doc_ids_string, _ = capsys.readouterr()
    doc_ids = doc_ids_string.strip().split()
    assert len(doc_ids) > 0
