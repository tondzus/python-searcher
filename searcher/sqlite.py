import os
import csv
import sqlite3

from searcher.controll import Controller
from searcher.document import GenericDocument


class SQLiteController(Controller):
    def __init__(self, config):
        self.__document_store = None
        self.__index_store = None
        self.config = config
        self.document_connector = config.get('sqlite3', 'documents')
        dbss = config.get('sqlite3', 'document_batch_store_size')
        self.document_batch_store_size = int(dbss)
        self.index_connector = config.get('sqlite3', 'indexes')

    @property
    def document_store(self):
        if self.__document_store is None:
            self.__document_store = SQLiteDocumentStore(self.document_connector)
        return self.__document_store

    @property
    def index_store(self):
        if self.__index_store is None:
            self.__index_store = SQLiteIndexStore(self.index_connector)
        return self.__index_store

    def index(self, use_spark=False):
        if use_spark:
            print('Can\'t use spark with SQLite datastores')
        else:
            super().index()


class SQLiteDocumentStore:
    def __init__(self, dbpath):
        self.dbpath = dbpath
        self.__db = None

    @property
    def db(self):
        if self.__db is None:
            self.__db = sqlite3.connect(self.dbpath)
        return self.__db

    def load_document(self, document_id):
        cur = self.db.cursor()
        query = 'SELECT content FROM documents WHERE id=?'
        result = cur.execute(query, (document_id, )).fetchone()
        return GenericDocument(document_id, result[0])

    def prepare_document_query(self, content):
        return (content, )

    def store_documents(self, contents):
        cur = self.db.cursor()
        query = 'INSERT INTO documents (content) VALUES (?)'
        cur.executemany(query, contents)
        self.db.commit()

    def __iter__(self):
        cur = self.db.cursor()
        query = 'SELECT id FROM documents'
        yield from (result[0] for result in cur.execute(query))

    def init(self):
        if os.path.isfile(self.dbpath):
            template = 'WARNING: {} already exist, won\'t overwrite'
            print(template.format(self.dbpath))
            return

        self.db.execute('CREATE TABLE documents('
                        'id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, '
                        'content TEXT NOT NULL);')

    def clear(self):
        if os.path.isfile(self.dbpath):
            print('WARNING: {} already exist, deleting'.format(self.dbpath))
            if self.__db is not None:
                self.__db.close()
            os.remove(self.dbpath)
            self.__db = None

    def __len__(self):
        return self.db.execute('SELECT COUNT(*) FROM documents').fetchone()[0]


class SQLiteIndexStore:
    def __init__(self, dbpath):
        self.dbpath = dbpath
        self.__db = None
        self.unsaved_indexes = []
        self.max_query_length = 10000
        self.next_index_id = 1
        self.csv_buffer = 'indexes.csv'
        self.csv_buffer_created = False

    @property
    def db(self):
        if self.__db is None:
            self.__db = sqlite3.connect(self.dbpath)
        return self.__db

    def register_document_indexes(self, index_document):
        self.unsaved_indexes.extend(index_document)
        if len(self.unsaved_indexes) > self.max_query_length:
            self.store_indexes(self.unsaved_indexes[:self.max_query_length])
            self.unsaved_indexes = self.unsaved_indexes[self.max_query_length:]

    def store_indexes(self, indexes=None):
        if not self.csv_buffer_created:
            if os.path.isfile(self.csv_buffer):
                os.remove(self.csv_buffer)
            self.csv_buffer_created = True

        if indexes is None:
            self.unsaved_indexes, indexes = [], self.unsaved_indexes

        indexes = [(iid, did, w, r)
                   for iid, (did, w, r) in enumerate(indexes, self.next_index_id)]
        self.next_index_id = indexes[-1][0] + 1

        with open(self.csv_buffer, 'a') as fp:
            writer = csv.writer(fp)
            writer.writerows(indexes)

    def flush(self):
        self.store_indexes(self.unsaved_indexes)
        self.unsaved_indexes = []

        msg = 'Generated indexes stored in {}. Please import to {} using ' \
              '\'.separator ","\' and \'.import {} indexes\' commads'
        data = self.csv_buffer, self.dbpath, self.csv_buffer
        print(msg.format(*data))
        msg = 'After import is done, you can delete {}'
        print(msg.format(self.csv_buffer))
        self.csv_buffer_created = False

    def find_by_word(self, word, limit=None):
        if limit:
            q = 'SELECT document_id, rank ' \
                'FROM indexes ' \
                'WHERE word=? ' \
                'ORDER BY rank DESC ' \
                'LIMIT {} '.format(limit)
        else:
            q = 'SELECT document_id, rank FROM indexes WHERE word=? ' \
                'ORDER BY rank DESC'
        cur = self.db.cursor()
        yield from cur.execute(q, (word, ))

    def init(self):
        if os.path.isfile(self.dbpath):
            template = 'WARNING: {} already exist, won\'t overwrite'
            print(template.format(self.dbpath))
            return

        self.db.execute('CREATE TABLE indexes('
                        'id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, '
                        'document_id INTEGER NOT NULL, '
                        'word CHAR(50) NOT NULL, '
                        'rank FLOAT NOT NULL);')
        self.db.execute('CREATE INDEX indexes_document_id_idx '
                        'ON indexes (document_id)')
        self.db.execute('CREATE INDEX indexes_word_idx ON indexes (word)')

    def clear(self):
        if os.path.isfile(self.dbpath):
            print('WARNING: {} already exist, deleting'.format(self.dbpath))
            if self.__db is not None:
                self.__db.close()
            os.remove(self.dbpath)
            self.__db = None
