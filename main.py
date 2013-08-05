#!/usr/bin/env python3

import queue
import itertools
import threading
from urllib.parse import urljoin

from grab import Grab


class Crawler(threading.Thread):

    def __init__(self, urls, maxtries=5, maxdepth=5):
        super(Crawler, self).__init__()
        self.urls = urls
        self.stop = threading.Event()
        self.tries = 0
        self.maxtries = maxtries
        self.g = Grab()
        self.maxdepth = maxdepth
        self.depth = 0

        self.baseurl = "http://en.wikipedia.org/"

    def add_url(self, href):
        clean_href = href.split("#")[0]
        if clean_href.startswith("/wiki"):
            self.urls.put(urljoin(self.baseurl, clean_href))

    def run(self):
        while True:
            try:
                msg = self.urls.get(timeout=5)
                print(msg)
            except queue.Empty:
                self.tries += 1
                if self.tries > self.maxtries:
                    print("Nothing to do here, exiting...")
                    break

            resp = self.g.go(msg)
            links = (
                l.get("href")
                for l in self.g.tree.cssselect("a[href]")
            )
            if not self.depth >= self.maxdepth:
                self.depth += 1
                list(map(self.add_url, links))


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
