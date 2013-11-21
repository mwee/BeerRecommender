import re
import sys

import pandas as pd

# One URL to rule them all, One URL to find them, One URL to bring them all and in the darkness bind them.
# Ok, well, maybe not darkness, but you get the idea.
BASE_URL = 'http://beeradvocate.com'

BEER_URL_REGEX = re.compile(r'http://beeradvocate.com/beer/profile/(?P<brewery_id>\d+)/(?P<beer_id>\d+)')
NEXT_TEXT_REGEX = re.compile('next &rsaquo;')

# the number of URLs after which we'll write out the results to a file
NUM_RESULTS_PER_FILE = 2000

def read_lines(file_name):
    """Return a list of lines from the file with the given name."""
    with open(file_name, 'r') as f:
        return [line.strip() for line in f]

def write_lines(file_name, array):
    """Write the list to the file, one element per line.

    If the file already exists, it will be overwritten.

    """
    with open(file_name, 'w') as f:
        for x in array:
            f.write(x + '\n')

def log(string):
    """Print the given string to the screen, padding it so that it can fully obscure a progress bar.

    Also prepend '[INFO]'. Because why not, I guess. Ok, I don't
    really know why, but we're logging shit, and it looks cool.

    """
    string = '[INFO] ' + string
    print string.ljust(150)

def print_progress_bar(x, y):
    """Print a progress bar with value (x / y) * 100.

    Allows itself to be overwritten by subsequent progress bar prints by
    ending with a carriage return, so that the progress bar appears to be
    animating as we print many of them. Sweeeeeeeeet.

    """
    percent = float(x) / float(y) * 100.0
    rounded_percent = int(round(percent))

    progress_bar_string = '[%s%s] %s / %s, %s%%' % ('#' * rounded_percent, ' ' * (100 - rounded_percent), x, y, percent)
    sys.stdout.write(progress_bar_string.ljust(150) + '\r')
    sys.stdout.flush()

def get_next_link(soup, url):
    """Return a URL to the next page in the paginated series, or None if there's no such link."""
    next_links = [x.findParent('a') for x in soup('a', text=NEXT_TEXT_REGEX)]
    if len(next_links) != 0 and len(next_links) != 2:
        print next_links
        print soup('a', text=NEXT_TEXT_REGEX)
        raise Exception("Number of 'next' links/spans (%s) not equal to 0 or 2! URL: %s" % (len(next_links), url))
    if len(next_links) == 0 or next_links[0] == None:
        return None
    else:
        return BASE_URL + next_links[0]['href']

def process_urls(urls, start, number_to_process, data_getter, filename_template):
    """Process the requested URLs, and save the scraped data to files.

    This is the main routine of the scrapers. It encapsulates the high-level
    loop and handles file writing.

    Parameters
    ----------
    urls : list of strings
        The URLs to process.
    start : int
        The index in urls at which to start.
    number_to_process : int
        The number of URLs to process. -1 to process all from start to the end of the list.
    data_getter : string (URL) => pandas.DataFrame
        The function used to process a URL. Should return a DataFrame representing a row of data.
    filename_template : string
        Template of the filename to use to save data. Should be a format string that can take
        one string  (i.e., one '%s').

    Returns
    -------
    None
        Just writes results to files; doesn't return anything.

    """
    # if -1 is given as the number of URLs to process, process all of them
    if number_to_process == -1:
        number_to_process = len(urls) - start

    # the dataframe in which we'll accumulate data
    df = pd.DataFrame()

    for i, url in enumerate(urls[start:]):
        print_progress_bar(i + 1, number_to_process)

        # make sure the URL has a trailing slash
        if url[-1] != '/':
            url += '/'

        # get the data for the current URL and add it to our dataframe
        df = df.append(data_getter(url), ignore_index=True)

        # if we've scraped enough URLs, write out to a file and reset the dataframe
        if (i + 1) % NUM_RESULTS_PER_FILE == 0:
            # compute file number based on overall position in the list of URLs
            file_number = ((start + i + 1) / NUM_RESULTS_PER_FILE) - 1
            file_number_str = '%02d' % file_number

            log('Writing to file %s' % file_number_str)
            df.to_csv(filename_template % file_number_str, encoding='utf-8', index=False)
            df = pd.DataFrame()

        # stop if we've scraped the desired number of beers
        if i == number_to_process - 1:
            break

    if not df.empty:
        log('Writing remaining data to one last file')
        df.to_csv(filename_template % 'last', encoding='utf-8', index=False)

    # print out progress bar just to make sure that it's displayed after the function returns
    print_progress_bar(number_to_process, number_to_process)
    print
