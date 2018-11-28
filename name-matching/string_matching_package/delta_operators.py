"""
Join ingested daily data with integrated data, keeping persistent group IDs.

This script outputs two dataframes:
1. Updated integrated data
  - Changed records that match with integrated data
  - New records that match with integrated data
2. Unmatched data
  - All records that do not match with integrated data

The following steps are performed per country:
- Pre-process integrated data to format for matching
- Pre-process ingested daily data to format for matching
- Match ingested data with integrated data
- Write two dataframes to file: updated integrated data and unmatched data
"""
import argparse

from string_matching.entity_delta_matching import delta_load_operators
from string_matching.entity_matching import preprocess_operators

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--integrated_input_path',
                        help='Path of the Parquet file with integrated operator data.')
    parser.add_argument('-g', '--ingested_daily_input_path',
                        help='Path of the Parquet file with ingested daily operator data.')
    parser.add_argument('-p', '--updated_integrated_output_path', default=None,
                        help='Write results in a Parquet file to this path.')
    parser.add_argument('-q', '--unmatched_output_path', default=None,
                        help='Write results in a Parquet file to this path.')
    parser.add_argument('-c', '--country_code', default='all',
                        help='Country code to use (e.g. US). Default all countries.')
    parser.add_argument('-t', '--threshold', default=0.9, type=float,
                        help='Drop similarities below this value [0-1].')
    parser.add_argument('-n', '--n_top', default=1500, type=int,
                        help='Keep N top similarities for each record.')
    # Note: because people do not understand the concept of distance we calculate
    # "Levenshtein similarity" as 1-Levenshtein distance. Be well aware of the fact this
    # is a non existent metric.
    parser.add_argument('-l', '--min_norm_name_levenshtein_sim', default=0.7, type=float,
                        help="Minimum normalised Levenshtein similarity [0-1, 0=unequal, 1=equal].")
    args = parser.parse_args()

    delta_load_operators(args, preprocess_operators)
