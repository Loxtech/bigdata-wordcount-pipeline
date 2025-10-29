#!/usr/bin/env python3

import sys
from itertools import groupby
from operator import itemgetter
 
def read_mapper_output(file, separator='\t'):
    """Reads tab-separated key-value pairs from a file."""
    for line in file:
        yield line.rstrip().split(separator, 1)
 
def main(separator='\t'):
    """Main function to read from stdin, aggregate counts, and print results."""
    # Group words and counts from stdin
    data = read_mapper_output(sys.stdin, separator=separator)
    for current_word, group in groupby(data, itemgetter(0)):
        try:
            # Sum counts for the current word
            total_count = sum(int(count) for _, count in group)
            print(f"{current_word}{separator}{total_count}")
        except ValueError:
            # count was not a number, so silently discard this item
            pass
 
if __name__ == "__main__":
    main()
