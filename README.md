# odf2csv
Command line tool for converting O-DF to CSV. **Needs python3.7 or newer.**

Usage
--------

usage: `odf2csv.py [-h] [--output OUTPUT] [--overwrite] [--xmlns ODFVERSION] [--omi-xmlns OMIVERSION] [--select-xpath SELECT] [--header-xpath HEADERSELECT] [--merge-columns] [--sort] [--verbose] [--version] o-df-file [o-df-file ...]`

Parses O-DF xml to csv format. It is essential that the xmlns is correct and the input O-DF data is sorted ascending in time (within an InfoItem, unless --sort flag is given).

positional arguments:
  * `o-df-file`           O-DF or O-MI file (in xml format, can be gzipped with ".gz" extension). Multiple files can be given.

optional arguments:
  * `-h`, `--help`            show this help message and exit
  * `--output OUTPUT`, `-o OUTPUT`
                        Output csv file
  * `--overwrite`, `-f`       Overwrite the output csv file if it already exists.
  * `--xmlns ODFVERSION`, `--ns ODFVERSION`, `--odf-xmlns ODFVERSION`
                        Xml namespace for the O-DF; This must match the uri in the input files, including the version number. Otherwise, xpath will not work. Will be assigned to "odf:" and "d" prefixes in XPath
  * `--omi-xmlns OMIVERSION`
                        Xml namespace for the O-MI if needed in XPath queries. Will be assigned to "omi:" and "m" prefixes in XPath
  * `--select-xpath SELECT`
                        XPath to select certain InfoItems from the O-DF (use "odf:" or "d:" as namespace for elements).
  * `--header-xpath HEADERSELECT`
                        XPath to select header title for each InfoItem. The XPath is run from each InfoItem (tip: recall the parent selector "../"). The result is further handled by "./odf:id/text()|@name" and results joined with '/'-character. The default is the
                        O-DF path to the item without the root /Objects/. Use "odf:" or "d:" as the namespace for elements.
  * `--merge-columns`       Merge columns with the same header name. By default, only combines columns having the same O-DF path and puts them in the order found.
  * `--sort`                Sorts the values in ascending order. This is needed, if they are not sorted in the input O-DF. Note: Loads all data to memory; not recommended for large files atm.
  * `--verbose`, `-v`         Print extra information during processing
  * `--version`             show program's version number and exit

