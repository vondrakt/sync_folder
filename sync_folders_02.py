#!/usr/bin/env python3

from optparse import OptionParser
import os
import time
import sqlite3

# defining the arguments that need to be passed to the script
arguments = OptionParser()

arguments.add_option('-s', '--source', dest='source', help='path to the source folder')
arguments.add_option('-r', '--replica', dest='replica', help='path to replica folder')
arguments.add_option('-p', '--period', dest='period', help='syncing period')
(options, args) = arguments.parse_args()

if (options.source is None or options.replica is None or options.period is None):
    # if one of the arguments is missing
    print('\n----------> A mandatory option is missing !\n')  # raise an error
    arguments.print_help()  # and provide the help
    exit(-1)  # exit the script
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Create a sqlite database with all the files from source.
source_database = sqlite3.connect('source_info.db')
source_database_cursor = source_database.cursor()

table_query = '''
filename TEXT NOT NULL,
path TEXT NOT NULL,
size INTEGER,
modified INTEGER
'''