import argparse
import csv
import itertools
import logging
import re
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
        self.base_url = 'http://running.competitor.com'

    def get_cache(self):
        try:
            socket.create_connection(('localhost', 11211))
            logger.info('using local memcached')
            return Client(['localhost:11211'])
        except socket.error:
            logger.info('no local memcached')

    def crawl(self, **params):
        generator = self.results_generator(**params)
        return list(itertools.chain(generator))

    def results_generator(self, **params):
        page = 1

        while True:
            params['page'] = page

            logger.info('querying for {}'.format(params))
            page_url = self.page_url(**params)
            html = self.query(page_url)
            results = list(self.parse_page(html))

            if not results:
                return

            yield results

            page += 1

    def page_url(self, city_id, year_id, event_id, page):
        return (
            '{base_url}/rnrresults?'
            'eId={city_id}'
            '&eiId={year_id}'
            '&seId={event_id}'
            '&resultsPage={page}'
            '&rowCount={page_count}'
        ).format(
            base_url=self.base_url,
            city_id=city_id,
            year_id=year_id,
            event_id=event_id,
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

        table = soup.find('div', {'class': 'rnrr_table_content'})
        links = table.findAll('a')

        for link in links:
            runner_url = self.base_url + link.get('href')

            regex = r'/rnrresults\?eId=\d+&eiId=\d+&seId=\d+&pId=\d+$'
            if not re.search(regex, runner_url):
                continue

            html = self.query(runner_url)
            yield self.parse_runner(html)

    def parse_runner(self, html):
        soup = BeautifulSoup(html)

        results = {}

        results['bib'] = int(soup.find('div', {'class': 'detail-bib'}).text)
        results['name'] = soup.find('div', {'class': 'detail-pptname'}).text

        details = soup.find('div', {'class': 'detail-pptlocation'})
        lis = details.findAll('li')
        results['location'] = lis[0].text
        results['age'] = int(lis[1].text.split(' ')[1])
        results['gender'] = lis[2].text.split(' ')[1]

        stats = soup.find('ul', {'class': 'detail-performance-stats'})
        lis = stats.findAll('li')

        results['overall_place'] = int(lis[0].find('a').text.strip())
        results['division_place'] = int(lis[1].find('a').text.strip())
        results['division'] = lis[1].find('a').get('href').split('=')[-1]
        results['gender_place'] = int(lis[2].find('a').text.strip())

        split_points = soup.find('ul', {'class': 'marker_points'})
        split_points = [li.text for li in split_points.findAll('li')]

        split_times = soup.find('ul', {'class': 'marker_points_times'})
        split_times = [li.text for li in split_times.findAll('li')]

        results.update({
            k.replace(' ', '_').lower(): v
            for k, v in zip(split_points, split_times)
        })

        finish_labels = soup.find('ul', {'class': 'marker_timing'})
        finish_labels = [li.text for li in finish_labels.findAll('li')]

        finish_values = soup.find('ul', {'class': 'marker_timing_times'})
        finish_values = [li.text.strip() for li in finish_values.findAll('li')]

        results.update({
            k.replace(' ', '_').lower(): v
            for k, v in zip(finish_labels, finish_values)
        })

        for k, v in results.iteritems():
            if isinstance(v, unicode):
                results[k] = v.encode('utf-8')

        return results


if '__main__' == __name__:
    parser = argparse.ArgumentParser(
        description='Crawl Rock n Roll race results'
    )

    default_filename = 'crawl.csv'
    parser.add_argument('--filename', default=default_filename,
                        help='output filename (default %s)' % default_filename)
    parser.add_argument('--city-id', default=54, help='city ID')
    parser.add_argument('--year-id', default=227, help='year ID')
    parser.add_argument('--event-id', default=791, help='event ID')

    args = parser.parse_args()

    results = Crawler().crawl(
        city_id=args.city_id,
        year_id=args.city_id,
        event_id=args.event_id
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
