import argparse
import csv
import itertools
import logging
import re
import requests
import socket

from bs4 import BeautifulSoup
from hashlib import md5
from memcache import Client


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Crawler(object):
    def __init__(self):
        self.cache = self.get_cache()
        self.page_count = 100
        self.base_url = 'http://www.runrocknroll.com'

    def get_cache(self):
        try:
            socket.create_connection(('localhost', 11211))
            logger.info('using local memcached')
            return Client(['localhost:11211'])
        except socket.error:
            logger.info('no local memcached')

    def crawl(self, **params):
        generator = self.results_generator(**params)
        return list(itertools.chain(*generator))

    def results_generator(self, **params):
        page = 1

        while True:
            params['page'] = page

            logger.info('querying for {}'.format(params))
            page_url = self.page_url(**params)
            results = self.query(page_url, self.parse_page)

            if not results:
                return

            yield results

            page += 1

    def page_url(self, event_id, subevent_id, page):
        return (
            '{base_url}/finisher-zone/search-and-results/?'
            'resultspage={page}'
            '&perpage={page_count}'
            '&eventid={event_id}'
            '&subevent_id={subevent_id}' 
        ).format(
            base_url=self.base_url,
            page=page,
            page_count=self.page_count,
            event_id=event_id,
            subevent_id=subevent_id
        )

    def query(self, url, parser):
        cache_key = md5(url).hexdigest()

        if self.cache:
            results = self.cache.get(cache_key)

            if results is not None:
                logger.info('retrieved {} from cache'.format(url))
                return results

        logger.info('querying {}'.format(url))
        html = self.query_server(url)
        results = parser(html)

        if self.cache:
            self.cache.set(cache_key, results, time=0)

        return results

    def query_server(self, url):
        response = requests.get(url)

        if 200 != response.status_code:
            raise RuntimeError((
                response.status_code, response.text
            ))

        return response.text

    def parse_page(self, html):
        soup = BeautifulSoup(html, 'html.parser')

        table = soup.find('table', {'class': 'fz_search_results_table'})
        links = table.findAll('a')

        runners = []
        for link in links:
            runner_url = self.base_url + link.get('href')
            runners.append(self.query(runner_url, self.parse_runner))

        return runners

    def parse_runner(self, html):
        soup = BeautifulSoup(html, 'html.parser')

        results = {}

        section_info = soup.find('section', {'id': 'finisher-info'})
        spans = section_info.findAll('span')

        results.update({
            'name': spans[1].text.strip(),
            'bib': int(spans[3].text.strip()),
            'gender': spans[5].text.strip(),
            'age': int(spans[7].text.strip()),
            'location': spans[9].text.strip(),
        })

        section_results = soup.find('section', {'id': 'finisher-results'})
        spans = section_results.findAll('span')

        def parse_or_none(x):
            x = x.text.replace('\\n', '').strip()
            return x if x and x != '-' else None

        results.update({
            'finish_time': parse_or_none(spans[1]),
            'goal_time': parse_or_none(spans[3]),
            'pace': parse_or_none(spans[-11]),
            'chip_time': parse_or_none(spans[-9]),
            'clock_time': parse_or_none(spans[-7]),
            'overall_place': parse_or_none(spans[-5]),
            'division_place': parse_or_none(spans[-3]),
            'gender_place': parse_or_none(spans[-1]),
        })

        split_step = 4 if 36 == len(spans) else 2
        split_spans = spans[5:5+split_step*5:split_step]

        results.update({
            split: parse_or_none(split_spans[i])
            for i, split in enumerate(('5km', '10km', '10mi', 'half', '20mi'))
        })
        
        for k, v in results.iteritems():
            if isinstance(v, unicode):
                results[k] = v.encode('utf-8')

        print(results)
        return results


if '__main__' == __name__:
    parser = argparse.ArgumentParser(
        description='Crawl Rock n Roll race results'
    )

    default_filename = 'crawl.csv'
    parser.add_argument('--filename', default=default_filename,
                        help='output filename (default %s)' % default_filename)
    parser.add_argument('--event-id', default=13, help='event ID')
    parser.add_argument('--subevent-id', default=1, help='subevent ID')

    args = parser.parse_args()

    results = Crawler().crawl(
        event_id=args.event_id,
        subevent_id=args.subevent_id
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
