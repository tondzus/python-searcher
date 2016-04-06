from collections import Counter


class DocumentIndexer:
    def __init__(self, document):
        self.total_word_count = 0
        self.document = document
        self.index = Counter()

    def index_document(self):
        for word in self.document:
            self.index[word] += 1
            self.total_word_count += 1

    def __iter__(self):
        did = self.document.document_id
        yield from ((did, w, float(c) / self.total_word_count)
                    for w, c in self.index.items())
