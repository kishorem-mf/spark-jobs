"""
Fuzzy matching of strings using Spark

General Flow:
- Read Parquet file.
- Pre-process dataframe to form a string column `name` which will contain the strings
  to be matched.
- Get countries with more than `n` entries.
- Strings are first tokenized using n-grams from the total corpus.
- Tokenized vector is normalized.
- Cosine similarity is calculated by absolute squaring the matrix.
- Collect N number of matches above a threshold.
- Group matches and assign a group ID.
- Write Parquet file partition by country code.
"""

import argparse

from string_matching.entity_matching import main_operators, preprocess_operators

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--input_file',
                        help='Input Parquet file path.')
    parser.add_argument('-p', '--output_path', default=None,
                        help='Write results in a Parquet file to this path.')
    parser.add_argument('-c', '--country_code', default='all',
                        help='Country code to use (e.g. US). Default all countries.')
    parser.add_argument('-frac', '--fraction', default=1.0, type=float,
                        help='Use this fraction of records.')
    parser.add_argument('-t', '--threshold', default=0.9, type=float,
                        help='Drop similarities below this value [0-1].')
    parser.add_argument('-n', '--n_top', default=1500, type=int,
                        help='keep N top similarities for each record.')
    # Note: because people do not understand the concept of distance we calculate
    # "Levenshtein similarity" as 1-Levenshtein distance. Be well aware of the fact this
    # is a non existent metric.
    parser.add_argument('-l', '--min_norm_name_levenshtein_sim', default=0.7, type=float,
                        help="Minimum normalised Levenshtein similarity [0-1, 0=unequal, 1=equal].")
    args = parser.parse_args()

    main_operators(args, preprocess_operators)
