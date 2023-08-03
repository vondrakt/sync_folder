#!/usr/bin/env python3

from optparse import OptionParser
import os
import time
from operator import itemgetter
import bisect

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

# Function for copying files
def copy_files(files, original_folder, target_folder):
    for i in files:
        original = '%s/%s' % (original_folder, i)
        target = '%s/%s' % (target_folder, i)
        os.system('cp %s %s' % (original, target))

# Function for deleting files
def delete_files(original_folder, target_folder):
    original_files = os.listdir(original_folder)
    target_files = os.listdir(target_folder)
    delete = list(set(target_files) - set(original_files))
    for i in delete:
        os.system('rm %s/%s' % (target_folder, i))

# source_files = [options.source+'/'+i for i in os.listdir(options.source)]
# source_times = [os.path.getmtime(i) for i in source_files]
# source_zipped = [list(x) for x in zip(source_files,source_times)]
# print(source_zipped)
# sorted_zipped = sorted(source_zipped, key = itemgetter(1))
# print(sorted_zipped)
#
# index = bisect.bisect_left(sorted_zipped, current_timestamp, key=itemgetter(1))
# print(index)

previous_sync = 1691060393.9171243
# if syncing was never done
if not previous_sync:
    # copy everything from source to replica
    source_files = os.listdir(options.source)
    source = options.source
    replica = options.replica
    copy_files(source_files,source,replica)
    # from replica delete what is not in source
    delete_files(options.source, options.replica)
    # now the folders should be identical
    # save the sync time
    previous_sync = time.time()
# if syncing was done
else:
    source_files = [options.source + '/' + i for i in os.listdir(options.source)]
    source_times = [os.path.getmtime(i) for i in source_files]
    source_zipped = [list(x) for x in zip(source_files, source_times)]
    sorted_zipped = sorted(source_zipped, key=itemgetter(1))
    index = bisect.bisect_left(sorted_zipped, previous_sync, key=itemgetter(1))
    print(index, sorted_zipped)
    print([i[0] for i in sorted_zipped[:index]])
    copy_files([i[0] for i in sorted_zipped[:index]], options.source, options.replica)
    delete_files(options.source, options.replica)
