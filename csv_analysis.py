import pandas as pd
import matplotlib.pyplot as plt

import argparse
import os

def main(args):
    # get all datasets
    files = os.listdir(args.data_dir)
    files.sort()
    # load data
    df = pd.DataFrame()
    for idx, filename in enumerate(files):
        if idx % 5 == 0:
            print("Progress: {}/{}".format(idx, len(files)))
        if filename.split('.')[-1] != "csv":
            continue
        data = pd.read_csv(args.data_dir + filename, delimiter='|', usecols=[args.stat_col])  # Use '|' as delimiter
        df = pd.concat([df, data], ignore_index=True)

    # get percentiles
    percentiles = [1 - precentile if args.revert_percentile else precentile for precentile in args.percentiles]
    percentile_values = df[args.stat_col].quantile(percentiles)
    for p, pv in zip(args.percentiles, percentile_values):
        if args.revert_percentile:
            print("Percentile {}: {}, Count: {}".format(p, pv, len(df[df[args.stat_col] >= pv])))
        else:
            print("Percentile {}: {}, Count: {}".format(p, pv, len(df[df[args.stat_col] <= pv])))
    # plot histogram for stat column
    plt.figure(figsize=(20, 6))
    plt.hist(data[args.stat_col], bins=100, alpha=0.7, color='green', edgecolor='black')

    # Mark specific percentile values on the histogram
    for p, pv in zip(args.percentiles, percentile_values):
        plt.axvline(pv, linestyle='--', color='red', label=f'{p:.1e}th percentile: {pv:.6f}')  
        
    plt.title('Histogram of {}'.format(args.stat_col))
    plt.xlabel(args.stat_col)
    plt.ylabel('Frequency')
    plt.legend()
    plt.savefig(args.image_output_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=str, default="/Users/wenqingwei/Desktop/Course/839-003/project/TPC-H1/", help="Taget directory")
    parser.add_argument("--image_output_path", type=str, default="/Users/wenqingwei/Desktop/Course/839-003/project/TPC-H1/histogram.png", help="Taget directory")
    parser.add_argument("--percentiles", type=list, default=[1e-7, 1e-6, 1e-5, 1e-4, 0.001, 0.01])
    parser.add_argument("--revert_percentile", type=bool, default=False)
    parser.add_argument("--stat_col", type=str, default="L_EXTENDEDPRICE")

    args = parser.parse_args()
    main(args)
