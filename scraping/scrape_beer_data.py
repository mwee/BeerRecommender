"""
# Scrape beer data.
"""

import argparse
import re

# from bs4 import BeautifulSoup
from BeautifulSoup import BeautifulSoup
import pandas as pd
import requests

from scraping_utils import BEER_URL_REGEX
from scraping_utils import read_lines, log, process_urls

BEER_STYLE_REGEX = re.compile(r'^/beer/style/\d+$')

ABV_REGEX = re.compile(r'^ \| &nbsp;(?P<abv>\d+(?:\.\d+)?)\% $')
UNKNOWN_ABV_REGEX = re.compile(r'^ \| &nbsp;ABV \?$')

NUM_RATINGS_REGEX = re.compile(r'^\nRatings: (?P<num_ratings>\d+)$')
NUM_REVIEWS_REGEX = re.compile(r'^Reviews: (?P<num_reviews>\d+)$')
RAVG_REGEX = re.compile(r'^rAvg: (?P<r_avg>\d+(?:\.\d+)?)$')
PDEV_REGEX = re.compile(r'^pDev: (?P<p_dev>\d+(?:\.\d+)?)\%$')

BEER_CSV_FILE_NAME_TEMPLATE = 'beers_new/beers_%s.csv'

# file containing beer URLs to be scraped
BEER_URLS_FILE = 'unique_beer_urls.txt'

def get_beer_info(beer_url, soup=None):
    # init a dict with default values for all attributes
    data_dict = {
        'brewery_id': None,
        'beer_id': None,
        'beer_name': '',
        'brewery_name': '',
        'syle': '',
        'abv': None,
        'ba_score': None,
        'bros_score': None,
        'num_ratings': None,
        'num_reviews': None,
        'r_avg': None,
        'p_dev': None,
        'alias_id': None,
        'alias_name': ''
    }

    # grab the main content div from the page
    if not soup:
        soup = BeautifulSoup(requests.get(beer_url).text)
    content_div = soup.find('div', attrs={'id': 'baContent'})

    # get brewery and beer IDs from the URL
    brewery_id, beer_id = BEER_URL_REGEX.match(beer_url).groups()
    data_dict['brewery_id'] = int(brewery_id)
    data_dict['beer_id'] = int(beer_id)

    # get the beer's name from the main <h1>
    data_dict['beer_name'] = soup.find('div', attrs={'class': 'titleBar'}).find('h1').contents[0]

    # is this beer is an alias for another, get alias info and return
    alias_regex = re.compile(r'This beer is an alias for <a href="/beer/profile/%s/(?P<alias_id>\d+)"><b>(?P<alias_name>.+)</b></a> from <a href="/beer/profile/%s"><b>(?P<brewery_name>.+)</b></a>\.' % (brewery_id, brewery_id))
    alias_match = alias_regex.search(unicode(content_div))
    if alias_match:        
        data_dict['brewery_name'] = alias_match.group('brewery_name')
        data_dict['alias_id'] = alias_match.group('alias_id')
        data_dict['alias_name'] = alias_match.group('alias_name')
        log('Alias: %s; %s/%s -> %s/%s' % (beer_url, brewery_id, beer_id, brewery_id, data_dict['alias_id']))

        return [pd.DataFrame({k: [data_dict[k]] for k in data_dict.keys()})]
    
    # find an <a> tag that contains the brewery name
    brewery_url_regex = re.compile(r'^/beer/profile/%s$' % brewery_id)
    data_dict['brewery_name'] = content_div.find('a', attrs={'href': brewery_url_regex}).text

    # <insert useless comment about how the next line gets the beer's style>
    data_dict['style'] = content_div.find('a', attrs={'href': BEER_STYLE_REGEX}).text

    # check if an ABV value is present
    abv_string = content_div.find(text=ABV_REGEX)
    if abv_string:
        abv_string = unicode(abv_string)
        data_dict['abv'] = float(ABV_REGEX.match(abv_string).group('abv'))
    else:
        # check if the ABV is noted as unknown;
        # if neither present nor unknown, something's up
        abv_string = content_div.find(text=UNKNOWN_ABV_REGEX)
        if not abv_string:
            log('Error finding ABV for URL %s ' % beer_url)

    # get the "BA SCORE" and the "THE BROS" score
    ratings_strings = [tag.text for tag in content_div.findAll('span', attrs={'class': 'BAscore_big'})]
    for name, rating in zip(['ba_score', 'bros_score'], ratings_strings):
        try:
            data_dict[name] = int(rating)
        except:
            # default values already set
            pass

    s = content_div.find(text=NUM_RATINGS_REGEX)
    if s:
        data_dict['num_ratings'] = int(NUM_RATINGS_REGEX.match(s).group('num_ratings'))
    else:
        log('Error finding num_ratings for URL %s' % beer_url)

    s = content_div.find(text=NUM_REVIEWS_REGEX)
    if s:
        data_dict['num_reviews'] = int(NUM_REVIEWS_REGEX.match(s).group('num_reviews'))
    else:
        log('Error finding num_reviews for URL %s' % beer_url)

    s = content_div.find(text=RAVG_REGEX)
    if s:
        data_dict['r_avg'] = float(RAVG_REGEX.match(s).group('r_avg'))
    else:
        log('Error finding r_avg for URL %s' % beer_url)

    s = content_div.find(text=PDEV_REGEX)
    if s:
        data_dict['p_dev'] = float(PDEV_REGEX.match(s).group('p_dev'))
    else:
        log('Error finding p_dev for URL %s' % beer_url)

    return [pd.DataFrame({k: [data_dict[k]] for k in data_dict.keys()})]

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape beer data')
    parser.add_argument('start', type=int, help='Index of first URL to process')
    parser.add_argument('num', type=int, help='Number of URLs to process. Use -1 to '
        'process all URLs from the given start index to the end of the list.')
    args = parser.parse_args()

    print 'Loading beer URLs...'
    beer_urls = read_lines(BEER_URLS_FILE)

    # for pretty-printing
    num_string = str(args.num)
    if args.num == -1:
        num_string = 'all (%s)' % (len(beer_urls) - args.start)

    log('Processing URLs. Start index: %s; Number to process: %s' % (args.start, num_string))
    process_urls(beer_urls, args.start, args.num, get_beer_info, [BEER_CSV_FILE_NAME_TEMPLATE])

