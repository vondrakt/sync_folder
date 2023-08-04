#!/usr/bin/env python3
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
    files = os.listdir(directory)
    path = [directory] * len(files)
    df_entry = [list(x) for x in zip(path,files)]
    df = pandas.DataFrame(df_entry,columns=['path', 'file ID'])
    return df

# Function to copy files
def copy_file(file, source_directory, target_directory):
    command = 'cp %s/%s %s' % (source_directory, file, target_directory)
    os.system(command)

# Function to delete files
def delete_file(file, directory):
    command = 'rm %s/%s' % (directory, file)
    os.system(command)

# Function that will calculate a checksum of file
def calculate_checksum(file, directory):
    with open(directory+'/'+file, 'rb') as f:
        data = f.read()
        md5_checksum = hashlib.md5(data).hexdigest()
        return(md5_checksum)


###################
# Open the log file to which the changes will be recorded.
out_log = open('./log.out', 'w')

# For the first sync, I will make an initial replica dataframe (df).
df_replica = make_df(options.replica)

# This while loop will execute the sync every given period p.
while 1:
    # Each loop I make a new source df.
    df_source = make_df(options.source)

    # To find the differences between the source and replica directories, I will concatenate their respective df.
    df_diff = pandas.concat([df_source, df_replica])

    # From the concatenated df, the duplicates are dropped to identify the differences.
    df_diff = df_diff.drop_duplicates(subset=['file ID'])

    # Iterating over the rows of the difference df.
    for index, row in df_diff.iterrows():

        # If the file exists in replica but not in source, it is deleted.
        if options.replica == row['path']:
            delete_file(row['file ID'], options.replica)
            message = 'Deleting file %s from %s.\n' % (row['file ID'], row['path'])
            print(message)
            out_log.write(message)

        # If the file exists in source...
        if options.source == row['path']:
            source_file = os.path.join(row['file ID'], row['path'])
            replica_file = os.path.join(row['file ID'], options.replica)

            # ... and in replica, their checksum values are compared.
            # If the checksum are different, the file is copied from source to replica.
            if os.path.isfile(replica_file):
                source_checksum = calculate_checksum(row['file ID'], options.source)
                replica_checksum = calculate_checksum(row['file ID'], options.replica)
                if source_checksum != replica_checksum:
                    copy_file(row['file ID'], row['path'], options.replica)
                    message = 'Copying file %s from %s to %s.\n' % (row['file ID'], row['path'], options.replica)

            # If the file exists in source but not in replica, the file is copied to replica
            else:
                copy_file(row['file ID'], row['path'], options.replica)
                message = 'Copying file %s from %s to %s.\n' % (row['file ID'], row['path'], options.replica)

            print(message)
            out_log.write(message)

    # The replica df is updated to match the source df, as these folders are now identical.
    df_replica = df_source
    # Wait for the next sync, for period p.
    time.sleep(int(options.period))

