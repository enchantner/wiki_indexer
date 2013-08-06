

from bson.objectid import ObjectId

from db import DBClient

class WordIndexer(object):

    def __init__(self):
        c = DBClient()
        self.db = getattr(c, c.db_name)

    def query(self, limit=10):
        result = []
        top = self.db.words_to_urls.find(
            {},
            sort=[("count", -1)],
            limit=limit
        )
        for wu in top:
            word_db = self.db.words.find_one({
                "_id": wu["word_id"]
            })
            result.append({
                "word": word_db["word"],
                "count": wu["count"]
            })
        return result
