#!/usr/bin/env python3
import datetime
import time
from optparse import OptionParser
import os
import pandas
import hashlib

# defining the arguments that need to be passed to the script
arguments = OptionParser()

arguments.add_option('-s', '--source', dest='source', help='path to the source folder')
arguments.add_option('-r', '--replica', dest='replica', help='path to replica folder')
arguments.add_option('-p', '--period', dest='period', help='syncing period, in seconds')
(options, args) = arguments.parse_args()

if (options.source is None or options.replica is None or options.period is None):
    # if one of the arguments is missing
    print('\n----------> A mandatory option is missing !\n')  # raise an error
    arguments.print_help()  # and provide the help
    exit(-1)  # exit the script
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Function that will create a pandas dataframe with the file IDs and timestamps
def make_df(directory):

    def get_datetime(file_ID, path):
        timestamp = os.path.getmtime(path+'/'+file_ID)
        res = datetime.datetime.fromtimestamp(timestamp)
        return res

    files = os.listdir(directory)
    df_entry = [[directory, i, get_datetime(i, directory)] for i in files]
    df = pandas.DataFrame(df_entry, columns=['path', 'file ID', 'timestamp'])
    return df

# Function to copy files
def copy_file(file, source_directory, target_directory):
    command = 'cp -r %s/%s %s' % (source_directory, file, target_directory)
    os.system(command)
    message = 'Copying %s from %s to %s.\n' % (file, source_directory, target_directory)
    print(message)
    out_log.write(message)

# Function to delete files
def delete_file(file, directory):
    command = 'rm %s/%s' % (directory, file)
    os.system(command)
    message = 'Deleting %s from %s.\n' % (file, directory)
    print(message)
    out_log.write(message)

# Function that will calculate a checksum of file
# def calculate_checksum(file, directory):
#     with open(directory+'/'+file, 'rb') as f:
#         data = f.read()
#         md5_checksum = hashlib.md5(data).hexdigest()
#         return(md5_checksum)


###################
# Open the log file to which the changes will be recorded.
out_log = open('./log.out', 'w')

timestamp = None
#print(timestamp)

# This while loop will execute the sync every given period p.
while 1:
    message = 'Sync at %s.\n' % datetime.datetime.now()
    print(message)
    out_log.write(message)

    # Make a source df.
    df_source = make_df(options.source)
    #print(df_source.to_string())
    # Make a replica df.
    df_replica = make_df(options.replica)
    #print(df_replica.to_string())

    # To find the differences between the source and replica directories, I will concatenate their respective df.
    df_diff = pandas.concat([df_source, df_replica])
    #print(df_diff.to_string())

    # From the concatenated df, find the duplicated files.
    df_duplicates = df_diff[df_diff.duplicated('file ID', keep=False)]
    #print(df_duplicates.to_string())

    # If the files are duplicated, they exist both in source and replica.
    if not df_duplicates.empty:
        for index, row in df_duplicates.iterrows():
            # To determine whether the file should be synced check the timestamp.
            # If the file exists in source and is newer than the previous sync, copy it to replica.
            if options.source == row['path'] and (timestamp is None or row['timestamp'] > timestamp):
                copy_file(row['file ID'], row['path'], options.replica)

                # A more accurate method of comparing files would be to calculate and compare their checksum.
                # However, this can be computationally demanding if the files are very large.
                # But if that is what we would want, then it could be done by commenting out the calculate_checksum function and the copy_file above and use the code below.

                # source_checksum = calculate_checksum(row['file ID'], options.source)
                # replica_checksum = calculate_checksum(row['file ID'], options.replica)

                # if source_checksum != replica_checksum:
                #     copy_file(row['file ID'], row['path'], options.replica)
                #     message = 'Copying file %s from %s to %s.\n' % (row['file ID'], row['path'], options.replica)
                #     print(message)
                #     out_log.write(message)

    # If the file names are unique in the concatenated dataframe, they should either be deleted from replica or copied from source.
    df_unique = df_diff.drop_duplicates('file ID', keep=False)
    #print(df_unique.to_string)

    if not df_unique.empty:
        for index, row in df_unique.iterrows():
            # If the file exists in source, copy it to replica.
            if options.source == row['path']:
                copy_file(row['file ID'], row['path'], options.replica)
            # If the file exists in replica, delete it.
            if options.replica == row['path']:
                delete_file(row['file ID'], row['path'])

    # Record the time of this sync
    timestamp = datetime.datetime.now()
    # Wait for the next sync, for period p.
    time.sleep(int(options.period))