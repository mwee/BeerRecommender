"""
# Parse a file returned from Amazon EMR, putting the data into a sqlite3 database.
"""

import sqlite3

# defines whether we're dealing with beers or users;
# possible values are 'beer' and 'user'
TYPE = 'beer'

# the source and destination file names
SOURCE_DATA_FILE = 'mr_output/output_all_full_%s_emr.txt' % TYPE
DATABASE_FILE_NAME = 'databases/%s_sim_database.db' % TYPE

if __name__ == '__main__':
    connection = sqlite3.connect(DATABASE_FILE_NAME)
    c = connection.cursor()

    # our db has one table, and each row contains a pair of object IDs along with their similarities and support
    c.execute("CREATE TABLE similarities (object_id_1 text, object_id_2 text, look real, smell real, taste real, feel real, overall real, support integer)")
    connection.commit()

    # explicitly start a transaction so that sqlite doesn't implicitly start and end transactions for each INSERT
    c.execute('BEGIN')

    with open(SOURCE_DATA_FILE) as f:
        line_counter = 0
        for line in f:
            if line_counter % 20000 == 0:
                print 'Processing line ' + str(line_counter)
            line_counter += 1

            # parse the line into its components
            parts = line.strip().split('\t')
            object_id_1, object_id_2 = eval(parts[0])
            data = eval(parts[1])
            look, smell, taste, feel, overall = data[0]

            c.execute("INSERT INTO similarities VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (object_id_1, object_id_2, look, smell, taste, feel, overall, data[1]))

    # IMPORTANT!!! Must commit in order for data insertion to persist.
    connection.commit()
    connection.close()
