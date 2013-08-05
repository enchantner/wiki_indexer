#!/usr/bin/env python3

import queue
import itertools
import threading
import argparse
from urllib.parse import urljoin

from grab import Grab
from pymongo import MongoClient


class Crawler(threading.Thread):

    def __init__(self, urls, maxtries=5, maxdepth=5):
        super(Crawler, self).__init__()
        self.urls = urls
        self.stop = threading.Event()
        self.g = Grab()

        self.tries = 0
        self.depth = 0
        self.maxtries = maxtries
        self.maxdepth = maxdepth

        self.baseurl = "http://en.wikipedia.org/"
        self.db = MongoClient('localhost', 27017)
        self.wiki = self.db.wiki

    def update_links(self, page):
        if not self.depth >= self.maxdepth:
            links = (
                l.get("href").split("#")[0]
                for l in page.cssselect("a[href]")
            )
            self.depth += 1
            list(map(self.add_url, links))

    def add_url(self, href):
        url = urljoin(self.baseurl, href)
        exist = self.wiki.parsed.find_one({"url": url})
        if not exist and href.startswith("/wiki"):
            self.urls.put(url)

    def save(self, url, page):
        entry = {
            "url": url,
            "content": page.cssselect("#mw-content-text")[0].text_content()
        }
        self.wiki.parsed.insert(entry)

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
            self.save(url, self.g.tree)
            self.update_links(self.g.tree)

def main():
    urls = queue.Queue()

    with open("start.txt", "r") as startlist:
        for url in startlist:
            urls.put(url)

    pool = [Crawler(urls) for _ in range(5)]
    list(map(
        lambda t: t.start(),
        pool
    ))


if __name__ == "__main__":
    main()
