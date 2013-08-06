
import re
import threading
import itertools
from urllib.parse import urljoin

from grab import Grab
from pymongo import MongoClient
from bson.objectid import ObjectId

from db import DBClient


class WikiCrawler(threading.Thread):

    def __init__(self, urls, maxtries=5, maxdepth=5):
        super(WikiCrawler, self).__init__()
        self.urls = urls
        self.stop = threading.Event()
        self.g = Grab()

        self.tries = 0
        self.depth = 0
        self.maxtries = maxtries
        self.maxdepth = maxdepth

        self.baseurl = "http://en.wikipedia.org/"
        c = DBClient()
        self.db = getattr(c, c.db_name)

        self.re_word = re.compile(r'\b\S+\b', re.U)
        self.exclude_words = [
            "the",
            "a",
            "an",
            "as",
            "or",
            "for",
            "of",
            "is",
            "be",
            "was",
            "are",
            "in",
            "to",
            "and",
            "not",
            "he",
            "she",
            "it"
        ]

    def update_urls(self, page):
        if not self.depth >= self.maxdepth:
            links = (
                l.get("href").split("#")[0]
                for l in page.cssselect("a[href]")
            )
            self.depth += 1
            list(map(self.add_url, links))

    def add_url(self, href):
        url = urljoin(self.baseurl, href)
        exist = self.db.urls.find_one({"url": url})
        if not exist and href.startswith("/wiki"):
            self.urls.put(url)

    def iter_words(self, page):
        content = self.get_content(page)
        for word in self.re_word.finditer(content):
            clean_word = word.group(0).lower()
            if clean_word in self.exclude_words:
                continue
            yield clean_word

    def get_content(self, page):
        return page.cssselect("#mw-content-text")[0].text_content()

    def _update_link(self, word_id, url_id):
        exist_w2u = self.db.words_to_urls.find_one({
            "word_id": word_id,
            "link_id": url_id
        })
        if exist_w2u:
            self.db.words_to_urls.update({
                "_id": exist_w2u["_id"]
            }, {
                "word_id": word_id,
                "link_id": url_id,
                "count": exist_w2u["count"] + 1
            })
        else:
            self.db.words_to_urls.insert({
                "word_id": word_id,
                "link_id": url_id,
                "count": 1
            })

    def save_word(self, word, url_db):
        exist_word = self.db.words.find_one({"word": word})
        if exist_word:
            self._update_link(exist_word["_id"], url_db["_id"])
        else:
            word_id = ObjectId()
            self.db.words.insert({
                "word": word,
                "_id": word_id
            })
            self._update_link(word_id, url_db["_id"])

    def process(self, url, page):
        url_id = self.db.urls.insert({
            "url": url
        })
        url_db = self.db.urls.find_one({
            "_id": url_id
        })
        for word in self.iter_words(page):
            self.save_word(word, url_db)
        self.update_urls(page)

    def run(self):
        while True:
            try:
                url = self.urls.get(timeout=5)
                print(url)
            except queue.Empty:
                self.tries += 1
                if self.tries > self.maxtries:
                    print("Nothing to do here, exiting...")
                    break

            resp = self.g.go(url)
            self.process(url, self.g.tree)
