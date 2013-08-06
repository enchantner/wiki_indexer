

from bson.objectid import ObjectId
from bson.code import Code

from db import DBClient

class WordIndexer(object):

    def __init__(self):
        c = DBClient()
        self.db = getattr(c, c.db_name)

    def query(self, limit=10):
        result = []
        if "words_to_urls" in self.db.collection_names():
            mapper = Code("""
                function() {
                    var key = {
                        word_id: this.word_id
                    };
                    emit(key, {
                        count: this.count
                    });
                }
            """)
            reducer = Code("""
                function(key, values) {
                    var sum = 0;
                    values.forEach(function(value) {
                        sum += value['count'];
                    });
                    return {
                        count: sum
                    };
                }
            """)
            mr_results = self.db.words_to_urls.map_reduce(
                mapper,
                reducer,
                "myresults"
            )
            top = mr_results.find(
                {},
                sort=[("value.count", -1)],
                limit=limit
            )
            for wu in top:
                word_db = self.db.words.find_one({
                    "_id": wu["_id"]["word_id"]
                })
                result.append({
                    "word": word_db["word"],
                    "count": wu["value"]["count"]
                })
        return result
