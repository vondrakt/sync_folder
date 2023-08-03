#!/usr/bin/env python3

from optparse import OptionParser
import os
import pandas
import hashlib

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
def calculate_checksum(file):
    with open(file, 'rb') as f:
        data = f.read()
        md5_checksum = hashlib.md5(data).hexdigest()
        return(md5_checksum)

# The tables need to have columns: file ID, timestamp, checksum

# Creating a pandas table that will contain source files and their file metadata
source_df = None

source_files = [options.source + '/' + i for i in os.listdir(options.source)]
for i in source_files:
    row = {'file ID': i, 'timestamp': os.path.getmtime(i), 'checksum': calculate_checksum(i)}
    #print(row)
    if source_df is None:
        source_df = pandas.DataFrame(row, index=[0])
        #print(source_df)
    else:
        row_df = pandas.DataFrame(row, index=[0])
        source_df = pandas.concat([source_df, row_df], ignore_index=True)

print(source_df.head())

