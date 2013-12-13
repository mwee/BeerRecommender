"""
# Concatenate the data files output by the scrapers into one big file.
"""

import argparse
from datetime import datetime
from os import listdir
from os.path import join, splitext
import re

import pandas as pd

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

def concatente_files(read_path, write_path, dest_file_name):
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
    new_file_path = join(write_path, dest_file_name)
    filenames = get_filenames(read_path)
    
    with open(new_file_path, 'w') as dest:
        current_file = 0
        for filename in filenames:
            file_path = join(read_path, filename)
            print '[INFO] Reading %s' % file_path

            with open(file_path, 'r') as source:
                # only get the column headers once
                if current_file > 0:
                    source.readline()
                current_file += 1

                # copy remaining lines from source to dest
                for line in source:
                    dest.write(line)
    
    return

    df = pd.DataFrame()

    for filename in get_filenames(read_path):
        file_path = join(read_path, filename)
        print '[INFO] Reading %s' % file_path

        df = pd.concat([df, pd.DataFrame.from_csv(file_path)])

    new_file_path = join(write_path, dest_file_name)
    print '[INFO] Writing to %s' % new_file_path
    df.to_csv(new_file_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Post-process scraped data')
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-c', '--concat', action='store_true', help='concatenate .csv files')
    group.add_argument('-t', '--timestamps', action='store_true', help='convert timestamps')

    parser.add_argument('-f', '--filename', required=False, help='The name of the file to which '
        'to write the result of file concatenation. Not required (and ignored if present) for -t/--timestamps.')

    parser.add_argument('source', help='Directory containing source .csv file(s)')
    parser.add_argument('dest', help='Directory to which to write produced .csv file(s)')
    args = parser.parse_args()

    if args.concat:
        if not args.filename:
            parser.error("argument -c/--concat requires argument -f/--filename.")
        concatente_files(args.source, args.dest, args.filename)
    else:
        convert_timestamps(args.source, args.dest)
