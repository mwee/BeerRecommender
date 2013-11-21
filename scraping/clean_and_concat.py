import argparse
from datetime import datetime
from os import listdir
from os.path import join, splitext
import re

import pandas as pd

def convert_timestamps(read_path, write_path):
    """Convert individual year, month, day, etc. fields into datetime.datetime instances.

    Read in the source .csv files, convert the year, month, day, hour, minute, and second
    columns into a column of datetime.datetime instances, drop the original columns, and
    write out the modified DataFrames to new .csv files. This function is necessary because the
    review scraping code does not produce datetimes.

    Parameters
    ----------
    read_path : string
        The path to the directory containing the source .csv files.
    write_path : string
        The directory to which to write the processed .csv files.

    Returns
    -------
    None

    """
    count = 0
    for filename in listdir(read_path):
        # only process .csv files
        _, file_extension = splitext(filename)
        if file_extension.lower() != '.csv':
            continue

        original_file_path = join(read_path, filename)
        new_file_path = join(write_path, 'scrubbed_' + filename)
        print '%s -> %s' % (original_file_path, new_file_path)

        df = pd.DataFrame.from_csv(original_file_path)
        df['timestamp'] = df.apply(lambda row: datetime(
                                  row['year'], row['month'], row['day'], row['hour'], row['minute'], row['second']), axis=1)

        df = df.drop('year', 1)
        df = df.drop('month', 1)
        df = df.drop('day', 1)
        df = df.drop('hour', 1)
        df = df.drop('minute', 1)
        df = df.drop('second', 1)

        df.to_csv(new_file_path, index=False)

def get_filenames(read_path):
    """Return a sorted list of the names of all .csv files in the given directory."""
    num_regex = re.compile(r'\d+')

    def sort_nicely(array):
        """Sort the given list in the way that humans expect.

        That is, if there are numbers in the filenames, sort
        them numerically instead of lexicographically.

        """
        alphanum_key = lambda s: [int(x) if x.isdigit() else x for x in num_regex.split(s)]
        return sorted(array, key=alphanum_key)

    filenames = []
    for f in listdir(read_path):
        # only get .csv files
        _, file_extension = splitext(f)
        if file_extension.lower() == '.csv':
            filenames.append(f)
    
    return sort_nicely(filenames)

def concatente_files(read_path, write_path):
    """Concatenate the .csv files, creating one big .csv file.

    Parameters
    ----------
    read_path : string
        The path to the directory containing the source .csv files.
    write_path : string
        The directory to which to write the concatenated .csv file.

    Returns
    -------
    None

    """
    df = pd.DataFrame()

    for filename in get_filenames(read_path):
        file_path = join(read_path, filename)
        print '[INFO] Reading %s' % file_path

        df = pd.concat([df, pd.DataFrame.from_csv(file_path)], ignore_index=True)

    new_file_path = join(write_path, 'concatenated_reviews.csv')
    df.to_csv(new_file_path, index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Post-process scraped data')
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-c', action='store_true', help='concatenate .csv files')
    group.add_argument('-t', action='store_true', help='convert timestamps')

    parser.add_argument('source', help='Directory containing source .csv file(s)')
    parser.add_argument('dest', help='Directory to which to write produced .csv file(s)')
    args = parser.parse_args()

    if args.c:
        concatente_files(args.source, args.dest)
    else:
        convert_timestamps(args.source, args.dest)

