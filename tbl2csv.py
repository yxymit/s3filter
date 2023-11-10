import os
import argparse
import glob

import csv 


def convert_tbl_to_csv(input_file, output_file, csv_header):
    with open(input_file, 'r', newline='\n', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='\n', encoding='utf-8') as outfile:

        reader = csv.reader(infile, delimiter='|')
        writer = csv.writer(outfile, delimiter='|')

        # start with header
        writer.writerow(csv_header.split("|"))
        for row in reader:
            writer.writerow(row)

def main(args):
    # get list of target file with prefix
    files = os.listdir(args.source_path)
    files.sort()
    for filename in files:
        if filename[: len(args.prefix)] == args.prefix and ".tbl" in filename:
            target_filename = filename.replace(".tbl", "") + ".csv"
            convert_tbl_to_csv(args.source_path + filename, args.target_path + target_filename, args.header)
            print("Convert {}".format(target_filename))


if __name__ == "__main__":

    header = "L_ORDERKEY|L_PARTKEY|L_SUPPKEY|L_LINENUMBER|L_QUANTITY|L_EXTENDEDPRICE|L_DISCOUNT|L_TAX|L_RETURNFLAG|L_LINESTATUS|L_SHIPDATE|L_COMMITDATE|L_RECEIPTDATE|L_SHIPINSTRUCT|L_SHIPMODE|L_COMMENT"
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_path", type=str, default="/Users/wenqingwei/Desktop/Course/839-003/project/TPC/tpch-kit/ref_data/", help="Source directory")
    parser.add_argument("--prefix", type=str, default="lineitem", help="Prefix of target files")
    parser.add_argument("--header", type=str, default=header, help="Prefix of target files")
    parser.add_argument("--target_path", type=str, default="/Users/wenqingwei/Desktop/Course/839-003/project/TPC-H1/", help="Taget directory")

    # Parse the command-line arguments
    args = parser.parse_args()
    main(args)