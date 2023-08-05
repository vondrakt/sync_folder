# sync_folder

Python script sync_folders.py can be used to sync two folders. Here the examples contain folders ./source and ./replica.
The comparison can be done by checking the timestamp changes of the data within the folders or by calculating the checksum values.
Calculating the checksum values can be time and memory intensive if the files are large. Because of this, the sync_folders.py script takes an optional argument --checksum to determine whether checksum comparisons should be made.
