
from pymongo import MongoClient


class DBClient(MongoClient):

    def __init__(self):
        super(DBClient, self).__init__('localhost', 27017)
        self.db_name = "wiki_index"
