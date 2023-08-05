#!/usr/bin/env python3
import checksumdir
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
arguments.add_option('-c', '--checksum', dest='checksum', help=' OPTIONAL True/False compare file checksum. Default is False.')
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
    # This nested function is calculates the datetime of files
    def get_datetime(file_ID, path):
        timestamp = os.path.getmtime(path+'/'+file_ID)
        res = datetime.datetime.fromtimestamp(timestamp)
        return res

    # First get the list of file IDs from the directory for which a df should be created.
    files = os.listdir(directory)
    # The data for the df has to be a list of lists.
    # Here we have the directory path, file name and datetime.
    df_entry = [[directory, i, get_datetime(i, directory)] for i in files]
    # Creating the df.
    df = pandas.DataFrame(df_entry, columns=['path', 'file ID', 'timestamp'])
    return df

# Function to copy files
def copy_file(file, source_directory, target_directory):
    # When copying data, it is important to know whether it is a file or folder.
    name_source = '%s/%s' % (source_directory, file)

    # If it is a file...
    if os.path.isfile(name_source):
        # Just copy it from the source to the target directory
        command = 'cp %s/%s %s' % (source_directory, file, target_directory)
        os.system(command)
        # Write the message to the command line and to the log file.
        message = 'Copying %s from %s to %s.\n' % (file, source_directory, target_directory)
        print(message)
        out_log.write(message)

    # If it is a directory...
    if os.path.isdir(name_source):
        name_target = '%s/%s' % (target_directory, file)
        # ... and it does not exist in the target directory, copy it recursively to the target.
        if not os.path.isdir(name_target):
            command = 'cp -r %s/%s %s' % (source_directory, file, target_directory)
            os.system(command)
            # Write the message to the command line and to the log file.
            message = 'Copying %s from %s to %s.\n' % (file, source_directory, target_directory)
            print(message)
            out_log.write(message)
        # ... if it does exist in target...
        else:
            # ... it first has to be deleted
            command = 'rm -r %s' % name_target
            os.system(command)
            # and then copied back into the target.
            command = 'cp -r %s/%s %s' % (source_directory, file, target_directory)
            os.system(command)
            # Write the message to the command line and to the log file.
            message = 'Copying %s from %s to %s.\n' % (file, source_directory, target_directory)
            print(message)
            out_log.write(message)

# Function to delete files
def delete_file(file, directory):
    # When deleting data, do it recursively in case of folders.
    command = 'rm -r %s/%s' % (directory, file)
    os.system(command)
    # Write the message to the command line and to the log file.
    message = 'Deleting %s from %s.\n' % (file, directory)
    print(message)
    out_log.write(message)

# Function that will calculate a checksum of file
def calculate_checksum(file, directory):
    # When calculating the checksum, it is important to know whether we are doing it
    # for a file or folder.

    # If it is a file, it has to be opened and read through.
    # Here I read through the whole file, but it could be done in chunks
    # and the function exited at the first detected change.
    # This would save on time and memory.
    if os.path.isfile(directory+'/'+file):
        with open(directory+'/'+file, 'rb') as f:
            data = f.read()
            md5_checksum = hashlib.md5(data).hexdigest()
            return md5_checksum

    # If it is a directory, a different library is used than for a file.
    if os.path.isdir(directory+'/'+file):
        md5_checksum = checksumdir.dirhash(directory+'/'+file)
        return md5_checksum

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
            if timestamp is None or row['timestamp'] > timestamp:

                # A more accurate method of comparing whether a file was updated is by comparing the checksums.
                # But this can be demanding if the files are very large.
                # Therefore, if the checksum option is True...
                if options.checksum == 'True':
                    # ...calculate the cheksum values for both files.
                    source_checksum = calculate_checksum(row['file ID'], options.source)
                    replica_checksum = calculate_checksum(row['file ID'], options.replica)
                    # If the chekcsum values are different, copy file from source to replica.
                    if source_checksum != replica_checksum:
                        copy_file(row['file ID'], options.source, options.replica)
                # if the checksum option is default, just copy the file.
                else:
                    copy_file(row['file ID'], options.source, options.replica)

    # If the file names are unique in the concatenated dataframe, they should either be deleted from replica or copied from source.
    df_unique = df_diff.drop_duplicates('file ID', keep=False)
    #print(df_unique.to_string)

    if not df_unique.empty:
        for index, row in df_unique.iterrows():
            # If the file exists in source, copy it to replica.
            if options.source == row['path']:
                copy_file(row['file ID'], options.source, options.replica)
            # If the file exists in replica, delete it.
            if options.replica == row['path']:
                delete_file(row['file ID'], options.replica)

    # Record the time of this sync
    timestamp = datetime.datetime.now()
    # Wait for the next sync, for period p.
    time.sleep(int(options.period))