import argparse
import csv
import itertools
import logging
import requests
import socket

from BeautifulSoup import BeautifulSoup
from hashlib import md5
from memcache import Client


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Crawler(object):
    def __init__(self):
        self.cache = self.get_cache()
        self.page_count = 100

    def get_cache(self):
        try:
            socket.create_connection(('localhost', 11211))
            logger.info('using local memcached')
            return Client(['localhost:11211'])
        except socket.error:
            logger.info('no local memcached')

    def crawl(self, event_id, sub_event_id):
        generator = self.results_generator(event_id, sub_event_id)
        return list(itertools.chain(generator))

    def results_generator(self, event_id, sub_event_id):
        page = 1

        while True:
            params = {
                'event_id': event_id,
                'sub_event_id': sub_event_id,
                'page': page,
            }

            logger.info('querying for {}'.format(params))
            url = self.page_url(**params)
            html = self.query(url)
            results = self.parse_page(html)

            if not results:
                return

            yield results

            page += 1

    def page_url(self, event_id, sub_event_id, page):
        return (
            'http://running.competitor.com/rnrresults?'
            'eId={event_id}'
            '&eiId={sub_event_id}'
            '&seId='
            '&resultsPage={page}'
            '&rowCount={page_count}'
            '&firstname=&lastname=&bib=&gender=&division=&city=&state='
        ).format(
            event_id=event_id,
            sub_event_id=sub_event_id,
            page=page,
            page_count=self.page_count
        )

    def query(self, url):
        cache_key = md5(url).hexdigest()

        if self.cache:
            html = self.cache.get(cache_key)

            if html is not None:
                logger.info('retrieved from cache')
                return html

        logger.info('querying server')
        html = self.query_server(url)

        if self.cache:
            self.cache.set(cache_key, html, time=0)

        return html

    def query_server(self, url):
        response = requests.get(url)

        if 200 != response.status_code:
            raise RuntimeError((
                response.status_code, response.text
            ))

        return response.text

    def parse_page(self, html):
        soup = BeautifulSoup(html)
        return []

    def parse_runner(self, html):
        pass


if '__main__' == __name__:
    parser = argparse.ArgumentParser(
        description='Crawl Boston Marathon results'
    )

    default_filename = 'crawl.csv'
    parser.add_argument('--filename', default=default_filename,
                        help='output filename (default %s)' % default_filename)
    parser.add_argument('--event-id', default=54, help='event ID')
    parser.add_argument('--sub-event-id', default=227, help='sub event ID')

    args = parser.parse_args()

    results = Crawler().crawl(
        event_id=args.event_id,
        sub_event_id=args.sub_event_id
    )

    logger.info('writing {} results to {}'.format(
        len(results),
        args.filename
    ))

    with open(args.filename, 'wb') as f:
        writer = csv.DictWriter(f, results[0].keys())
        writer.writeheader()

        for result in results:
            writer.writerow(result)
