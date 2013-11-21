import argparse
import re

# from bs4 import BeautifulSoup
from BeautifulSoup import BeautifulSoup
import pandas as pd
import requests

from scraping_utils import BEER_URL_REGEX, BASE_URL
from scraping_utils import read_lines, write_lines, log, print_progress_bar, get_next_link, process_urls

DIRECTORY_URL = BASE_URL + '/beerfly/directory?show=all'

BREWERY_LINK_REGEX = re.compile('Breweries \(\d+\)')

float_string = r'\d+(?:\.\d+)?'
FLOAT_REGEX = re.compile(r'^' + float_string + r'$')
RDEV_REGEX = re.compile(r'rDev(?: )?((?:-|\+)?%s)%%' % float_string)
SUBRATING_REGEX = re.compile(r'^look: (%s) \| smell: (%s) \| taste: (%s) \| feel: (%s) \|(?: ){1,2}overall: (%s)$' % (float_string, float_string, float_string, float_string, float_string))
TIMESTAMP_REGEX = re.compile(r'^(?P<month>\d{2})-(?P<day>\d{2})-(?P<year>\d{4}) (?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})')
SERVING_TYPE_REGEX = re.compile(r'^Serving type: (?P<serving_type>[a-zA-Z\-]*)$')

# constants for file output
CSV_FILE_NAME_TEMPLATE = 'reviews/reviews_%s.csv'

def get_brewery_listing_urls(url):
    """Return a list of the brewery listing URLs, starting on the given page.

    Recursively explore tha page tree until brewery listing URLs have
    been found for all regions.

    Parameters
    ----------
    url : string
        A URL that points to a region listing page.

    Returns
    -------
    list
        A list of strings (URLs) pointing to brewery listing pages.

    """
    soup = BeautifulSoup(requests.get(url).text)
    table = soup.find('span', text='Categories').findNext('table')
    
    brewery_link = soup.find('a', text=BREWERY_LINK_REGEX)
    if brewery_link:
        try:
            return [BASE_URL + soup.find('a', text=BREWERY_LINK_REGEX).findParent('a')['href']]
        except:
            return []

    urls = []
    for link in [BASE_URL + a['href'] for a in table('a')]:
        urls += get_brewery_listing_urls(link)
    return urls

def get_brewery_urls(listing_url):
    """Return a list of brewery URLs, starting on the given brewery listing page.

    Recursively visit any 'next' pages linked to from the start page,
    until there are no more 'next' pages.

    Parameters
    ----------
    listing_url : string
        A URL that points to a brewery listing page.

    Returns
    -------
    list
        A list if strings (URLs) pointing to brewery pages.

    """
    soup = BeautifulSoup(requests.get(listing_url).text)

    brewery_urls = [BASE_URL + a['href'] for a in soup('a', attrs={'href': re.compile('/profile/\d+')})]

    # recursively process any additional pages of listings
    next_link = get_next_link(soup, listing_url)
    if next_link:
        return brewery_urls + get_brewery_urls(next_link)
    else:
        return brewery_urls

def get_beer_urls(brewery_url):
    """Return a list of beer URLs on the given brewery page.

    Parameters
    ----------
    brewery_url : string
        A URL that points to a brewery page.

    Returns
    -------
    list
        A list of strings (URLs) pointing to beer pages.

    """
    soup = BeautifulSoup(requests.get(brewery_url).text)
    beer_urls = [BASE_URL + a['href'] for a in soup('a', attrs={'href': re.compile('beer/profile/\d+/\d+$')})]
    return beer_urls

def get_reviews(beer_url):
    """Return a DataFrame of reviews for the beer with the given URL.

    Recursively visit any 'next' pages linked to from the main page,
    until there are no more 'next' pages.

    Parameters
    ----------
    beer_url : string
        A URL that points to a beer page.

    Returns
    -------
    DataFrame
        A DataFrame of reviews of the beer.

    """
    soup = BeautifulSoup(requests.get(beer_url, params={'show_ratings': 'Y'}).text)

    brewery_id, beer_id = BEER_URL_REGEX.match(beer_url).groups()
    id_dict = {'brewery_id': int(brewery_id), 'beer_id': int(beer_id)}

    df = pd.DataFrame() 
    for div in soup('div', attrs={'id': 'rating_fullview_content_2'}):
        attrs = extract_review_content(div)
        attrs.update(id_dict)

        row = pd.DataFrame({k: [attrs[k]] for k in attrs.keys()})
        df = df.append(row, ignore_index=True)

    # recursively process any additional pages of reviews
    next_link = get_next_link(soup, beer_url)
    if next_link:
        return df.append(get_reviews(next_link), ignore_index=True)
    else:
        return df

def extract_review_content(div):
    """Return a dictionary of review data from the given HTML div.

    This is the function that actually does the review scraping.

    Parameters
    ----------
    div : BeautifulSoup.Tag
        A div containing a single review.

    Returns
    -------
    dict
        A dict of review data.

    """
    html = unicode(div)
    text_content = div.text

    # split the html on breaks
    split_content = []
    for s in html.split('<br />'):
        if s != '':
            split_content.append(s)

    attrs = {}
    attrs['username'] = div.find('h6').text
    attrs['rating'] = float(div.find('span', {'class': 'BAscore_norm'}).text)
    attrs['rDev'] = float(RDEV_REGEX.search(text_content).group(1))

    # get user location if it's present
    attrs['user_location'] = None
    if not split_content[1].startswith('<'):
        attrs['user_location'] = split_content[1]

        # cut off the first part of the array so that it's easier to deal with presence/lack of location
        split_content = split_content[3:]
    else:
        # cut off the first part of the array so that it's easier to deal with presence/lack of location
        split_content = split_content[2:]

    # get subratings if they're present
    # presence of subratings also indicates presence of serving type and text review
    match = SUBRATING_REGEX.match(split_content[0])
    attrs.update(zip(['look', 'smell', 'taste', 'feel', 'overall'], [None, None, None, None, None]))
    attrs['serving_type'] = None
    attrs['text'] = None
    if match:
        attrs.update(zip(['look', 'smell', 'taste', 'feel', 'overall'], [float(r) for r in match.groups()]))

        # get serving type
        serving_type_index = -2
        found_serving_type = False
        while not found_serving_type:
            # print 'INDEX ', serving_type_index
            match = SERVING_TYPE_REGEX.match(split_content[serving_type_index])
            if match:
                found_serving_type = True
                attrs['serving_type'] = match.group('serving_type')
            else:
                serving_type_index -= 1

            # handle the case where the serving type is not present even though this is a full-text review;
            # kind of a hack, but whatever
            if abs(serving_type_index) <= len(split_content):
                serving_type_index = -1
                break
        
        # get text of review
        attrs['text'] = '\n'.join(split_content[1:serving_type_index])

    # get timestamp
    match = TIMESTAMP_REGEX.match(split_content[-1])
    attrs.update(zip(['month', 'day', 'year', 'hour', 'minute', 'second'], [int(x) for x in match.groups()]))

    return attrs

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape beer reviews')
    parser.add_argument('start', type=int, help='Index of first URL to process')
    parser.add_argument('num', type=int, help='Number of URLs to process. Use -1 to '
        'process all URLs from the given start index to the end of the list.')
    args = parser.parse_args()

    print 'Getting brewery listing URLs...'
    # listing_urls = get_brewery_listing_urls(DIRECTORY_URL)
    # write_lines('listing_urls.txt', listing_urls)
    listing_urls = read_lines('listing_urls.txt')

    print 'Getting brewery URLs...'
    # brewery_urls = []
    # for i, url in enumerate(listing_urls):
    #     print_progress_bar(i + 1, len(listing_urls))
    #     brewery_urls += get_brewery_urls(url)
    # write_lines('brewery_urls.txt', brewery_urls)
    brewery_urls = read_lines('brewery_urls.txt')

    print 'Getting beer URLs...'
    # beer_urls = []
    # for i, url in enumerate(brewery_urls):
    #     # print 'Processing brewery %s' % i
    #     print_progress_bar(i + 1, len(brewery_urls))
    #     beer_urls += get_beer_urls(url + '/?view=beers&show=all')
    # write_lines('beer_urls.txt', beer_urls)
    # beer_urls = set(read_lines('beer_urls.txt'))
    # write_lines('unique_beer_urls.txt', beer_urls)
    beer_urls = read_lines('unique_beer_urls.txt')

    # for pretty-printing
    num_string = str(args.num)
    if args.num == -1:
        num_string = 'all (%s)' % (len(beer_urls) - args.start)

    print 'Getting beer reviews...'
    log('Processing URLs. Start index: %s; Number to process: %s' % (args.start, num_string))
    process_urls(beer_urls, args.start, args.num, get_reviews, CSV_FILE_NAME_TEMPLATE)

