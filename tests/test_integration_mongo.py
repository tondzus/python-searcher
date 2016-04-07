import os
import pytest
import pymongo
import configparser
from operator import itemgetter
from searcher.mongo import MongoController


@pytest.fixture(scope='module')
def config(request):
    cnf = configparser.ConfigParser()
    cnf['default'] = {'datastore': 'mongo',
                      'query_limit': 10}
    cnf['mongo'] = {'host': 'localhost',
                    'document_batch_store_size': 100,
                    'port': 27017,
                    'dbname': 'test_pyse'}
    return cnf


@pytest.fixture(scope='module')
def mongo(config):
    host, port = config.get('mongo', 'host'), config.get('mongo', 'port')
    return pymongo.MongoClient('mongodb://{}:{}'.format(host, port))


@pytest.fixture(scope='function')
def db(config, mongo):
    return mongo[config.get('mongo', 'dbname')]


@pytest.fixture(scope='function', autouse=True)
def clean_resources(request, config, db):
    def fin():
        db.documents.drop()
        db.indexes.drop()
    request.addfinalizer(fin)


@pytest.fixture(scope='function')
def document_root(request):
    res = os.path.join(str(request.config.rootdir), 'tests', 'resources')
    return os.path.join(res, 'documents')


@pytest.fixture(scope='function')
def controller(config):
    return MongoController(config)


@pytest.fixture(scope='function')
def controller_init(controller):
    controller.init(force=True)
    return controller


@pytest.fixture(scope='function')
def controller_docs(controller_init, document_root):
    controller_init.register(root=document_root)
    return controller_init


@pytest.fixture(scope='function')
def controller_idx(config, controller_docs):
    controller_docs.index()
    return controller_docs


def test_database_init(config, controller):
    controller.init(force=False)


def test_database_init_db_exist(config, controller, db):
    db.documents.insert({'id': 1, 'content': 'some text'})
    controller.init(force=False)
    documents = list(db.documents.find())
    assert len(documents) == 1
    assert documents[0]['id'] == 1


def test_database_force_init(config, controller, db):
    db.documents.insert({'id': 1, 'content': 'some text'})
    document_count = db.documents.find().count()
    assert document_count == 1

    controller.init(force=True)
    document_count = db.documents.find().count()
    assert document_count == 0


def test_document_register(controller_init, document_root, db):
    controller_init.register(root=document_root)
    assert db.documents.find().count() == len(os.listdir(document_root))


def test_document_index(controller_docs, db):
    controller_docs.index()
    index_count = db.indexes.find().count()
    assert index_count > 0


def test_document_index_no_duplicates(controller_docs, db):
    controller_docs.index()
    indexes = list(map(itemgetter('word'), db.indexes.find()))
    indexes_set = set(indexes)
    assert len(indexes) == len(indexes_set)


def test_document_show_content(controller_docs, db, capsys):
    document = db.documents.find_one()
    controller_docs.show(document_ids=[document['_id']], preview=False)
    content, _ = capsys.readouterr()
    assert content[:-1] == document['content']


def test_document_show_preview(controller_docs, db, capsys):
    document = db.documents.find_one()
    controller_docs.show(document_ids=[document['_id']], preview=True)
    content, _ = capsys.readouterr()
    assert content[:-1] in document['content']


def test_query(controller_idx, db, capsys):
    index = db.indexes.find_one()
    controller_idx.query(index['word'], measure=False, preview=False)
    doc_ids_string, _ = capsys.readouterr()
    doc_ids = doc_ids_string.strip().split()
    assert len(doc_ids) > 0
