from struct import pack
from operator import itemgetter
from bson.objectid import ObjectId
from pyspark import SparkContext
from searcher.controll import load_config
from searcher.document import GenericDocument


def get_object_id(java_dict):
    ts = java_dict['timestamp']
    mi = java_dict['machineIdentifier']
    pi = java_dict['processIdentifier']
    co = java_dict['counter']
    bin_str = pack('>I', ts) + pack('>I', mi)[1:] + \
              pack('>I', pi)[2:] + pack('>I', co)[1:]
    return ObjectId(bin_str)


def index_document(mongo_document):
    #obj_id = get_object_id(mongo_document['_id'])
    obj_id = mongo_document['_id']
    document = GenericDocument(str(obj_id), mongo_document['content'])
    document.indexer.index_document()
    return [(None, {'word': i[1], 'hit': {'document': i[0], 'rank': i[2]}})
            for i in document.indexer]


def join_hits(hit1, hit2):
    hits = hit1['hits'] + hit2['hits']
    return {'hits': sorted(hits, key=itemgetter('rank'), reverse=True)}


def index():
    input_fmt_cls_name = 'com.mongodb.hadoop.MongoInputFormat'
    output_fmt_cls_name = 'com.mongodb.spark.PySparkMongoOutputFormat'
    val_cls_name = key_cls_name = 'com.mongodb.hadoop.io.BSONWritable'
    val_converter = key_converter = 'com.mongodb.spark.pickle.NoopConverter'

    config = load_config()
    host, port = config.get('mongo', 'host'), config.get('mongo', 'port')
    dbname = config.get('mongo', 'dbname')
    dbpath_in = 'mongodb://{}:{}/{}.documents'.format(host, port, dbname)
    dbpath_out = 'mongodb://{}:{}/{}.indexes_raw'.format(host, port, dbname)

    sc = SparkContext('local', 'pyspark')
    doc_rdd_raw = sc.newAPIHadoopRDD(input_fmt_cls_name, key_cls_name,
                                     val_cls_name, None, None,
                                     {'mongo.input.uri': dbpath_in})
    doc_rdd = doc_rdd_raw.values()

    result = doc_rdd.flatMap(index_document)#.reduceByKey(join_hits)
    #result.coalesce(1, True).saveAsTextFile('results')
    result.saveAsNewAPIHadoopFile(
        'file:///placeholder',
        outputFormatClass=output_fmt_cls_name,
        keyClass=key_cls_name,
        valueClass=val_cls_name,
        keyConverter=key_converter,
        valueConverter=val_converter,
        conf={'mongo.output.uri': dbpath_out})


if __name__ == '__main__':
    index()
