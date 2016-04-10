import os
from os.path import expanduser, abspath
import sys
from bson.objectid import ObjectId
from plumbum import local
from pymongo import MongoClient

from searcher.controll import Controller
from searcher.document import GenericDocument


class MongoController(Controller):
    def __init__(self, config):
        self.__document_store = None
        self.__index_store = None
        self.config = config
        host, port = config.get('mongo', 'host'), config.get('mongo', 'port')
        self.dbpath = 'mongodb://{}:{}'.format(host, port)
        self.db = MongoClient(self.dbpath)
        dbss = config.get('mongo', 'document_batch_store_size')
        self.document_batch_store_size = int(dbss)

    @property
    def document_store(self):
        if self.__document_store is None:
            dbname = self.config.get('mongo', 'dbname')
            self.__document_store = MongoDocumentStore(self.db, dbname)
        return self.__document_store

    @property
    def index_store(self):
        if self.__index_store is None:
            dbname = self.config.get('mongo', 'dbname')
            self.__index_store = MongoIndexStore(self.db, dbname)
        return self.__index_store

    def init(self, force):
        if not force:
            print('No need to init mongo datastore unless you want to '
                  'clear existing datastores using --force option')
            return

        self.document_store.clear()
        self.index_store.clear()

    def index(self, use_spark=False):
        if use_spark:
            os.putenv('PYSPARK_PYTHON', sys.executable)
            spark_indexer = local.which('spark-indexer.py')
            spark = self.prepare_spark_cmd()
            print('indexing documents using spark...', end='\r')
            spark(str(spark_indexer))
            print('indexed all documents from datastore')
            self.index_store.optimize_datastore()
        else:
            super().index()

    def prepare_spark_cmd(self):
        spark_root = self.config.get('mongo', 'spark', fallback='')
        cmd = os.path.join(spark_root, 'spark-submit')
        mongo_jar = self.config.get('mongo', 'spark-mongo')
        mongo_jar = abspath(expanduser(mongo_jar))
        return local[cmd]['--jars', mongo_jar]


class MongoDocumentStore:
    def __init__(self, mongoclient, dbname):
        self.db = mongoclient
        self.dbname = dbname

    def prepare_document_query(self, content):
        return {'content': content}

    def store_documents(self, contents):
        documents = self.db[self.dbname].documents
        documents.insert_many(contents)

    def load_document(self, document_id):
        documents = self.db[self.dbname].documents
        document = documents.find_one({'_id': ObjectId(document_id)})
        return GenericDocument(document_id, document['content'])

    def __iter__(self):
        documents = self.db[self.dbname].documents
        yield from (res['_id'] for res in documents.find(projection={}))

    def clear(self):
        db = self.db[self.dbname]
        collections = db.collection_names()
        if 'documents' in collections:
            print('WARNING: documents database already exists, droping')
            db.documents.drop()

    def __len__(self):
        return self.db[self.dbname].documents.count()


class MongoIndexStore:
    def __init__(self, mongoclient, dbname):
        self.db = mongoclient
        self.max_query_length = 30000
        self.unsaved_indexes = []
        self.dbname = dbname

    def register_document_indexes(self, index_document):
        unsaved = [{'word': word, 'hit': {'document': doc_id, 'rank': rank}}
                   for doc_id, word, rank in index_document]
        self.unsaved_indexes.extend(unsaved)
        if len(self.unsaved_indexes) > self.max_query_length:
            self.store_indexes(self.unsaved_indexes[:self.max_query_length])
            self.unsaved_indexes = self.unsaved_indexes[self.max_query_length:]

    def store_indexes(self, indexes=None):
        if indexes is None:
            self.unsaved_indexes, indexes = [], self.unsaved_indexes
        self.db[self.dbname].indexes_raw.insert_many(indexes)

    def flush(self):
        self.store_indexes(self.unsaved_indexes)
        self.unsaved_indexes = []
        self.optimize_datastore()

    def optimize_datastore(self):
        print('optimizing datastore...', end='\r')
        indexes_raw = self.db[self.dbname].indexes_raw
        pipeline = [
            {'$sort': {'hit.rank': -1}},
            {'$group': {'_id': '$word', 'hits': {'$push': '$hit'}}},
            {'$project': {'word': '$_id', 'hits': 1}},
            {'$out': 'indexes'}, ]
        indexes_raw.aggregate(pipeline, allowDiskUse=True, useCursor=False)
        indexes_raw.drop()
        print('datastore optimization done')

    def find_by_word(self, word, limit=None):
        indexes = self.db[self.dbname].indexes
        if limit:
            projection = {'hits': {'$slice': limit}, '_id': 0}
        else:
            projection = {'hits': 1, '_id': 0}
        res = indexes.find_one({'_id': word}, projection=projection)
        if res:
            yield from ((str(r['document']), r['rank']) for r in res['hits'])
        else:
            return []

    def clear(self):
        db = self.db[self.dbname]
        collections = db.collection_names()
        if 'indexes' in collections:
            print('WARNING: indexes database already exists, droping')
            db.indexes.drop()
        if 'indexes_raw' in collections:
            db.indexes_raw.drop()
