import os
import argparse
import glob

import pandas as pd

def convert(file_path, header_line):
    # read in
    # save as csv
    pass

def main(args):
    # get list of target file with prefix
    # convert
    pass

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--source_path", type=str, default="lineitem", help="Taget directory")
    parser.add_argument("--prefix", type=str, default="lineitem", help="Prefix of target files")
    parser.add_argument("--header", type=str, help="Prefix of target files")
    parser.add_argument("--target_path", type=str, default="lineitem", help="Taget directory")

    # Parse the command-line arguments
    args = parser.parse_args()
    main(args)