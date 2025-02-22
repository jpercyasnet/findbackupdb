# findbackupdb
Pure Rust program: Read the screen and formated list of windows files and see if they exist in the Backup Database. Output files reflecting finds and not founds. This is used to restore files.

example:

findbackup01 bk20240531061717.db3 20250217List.csv_out02.csv

bk20240531061717.db3 is backup database 

20250217List.csv_out02.csv is the screen and formated list of windows files from windirparse

very slow because of database calls.
