import sqlite3
from searcher import utils
from searcher import index


class GenericDocument:
    """Representation of stored document. Can be queried for more information
    about this document and can be used to iterate over all words used in this
    document.
    """
    def __init__(self, document_id, content, meta={}):
        self.document_id = document_id
        self.content = content
        self.meta = meta
        self.__indexer = None

    def __iter__(self):
        yield from utils.iterate_words(self.content)

    @property
    def indexer(self):
        if self.__indexer is None:
            self.__indexer = index.DocumentIndexer(self)
        return self.__indexer


class SQLiteDocumentStore:
    def __init__(self, dbpath):
        self.dbpath = dbpath
        self.conn = sqlite3.connect(dbpath)

    def load_document(self, document_id):
        cur = self.conn.cursor()
        query = 'SELECT content FROM documents WHERE id=?'
        result = cur.execute(query, (document_id, )).fetchone()
        return GenericDocument(document_id, result[0])

    def __iter__(self):
        cur = self.conn.cursor()
        query = 'SELECT id FROM documents'
        yield from (result[0] for result in cur.execute(query))
