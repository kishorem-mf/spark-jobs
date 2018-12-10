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

from string_matching.entity_delta_matching import (delta_load_contactpersons)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--integrated_input_path',
                        help='full path or location of the parquet file with integrated contact person data')
    parser.add_argument('-g', '--ingested_daily_input_path',
                        help='full path or location of the parquet file with ingested daily contact person data')

    parser.add_argument('-p', '--updated_integrated_output_path', default=None,
                        help='write results in a parquet file to this full path or location directory')
    parser.add_argument('-q', '--unmatched_output_path', default=None,
                        help='write results in a parquet file to this full path or location directory')

    parser.add_argument('-c', '--country_code', default='all',
                        help='country code to use (e.g. US). Default all countries.')
    parser.add_argument('-t', '--threshold', default=0.9, type=float,
                        help='drop similarities below this value [0.-1.].')
    parser.add_argument('-n', '--n_top', default=1500, type=int,
                        help='keep N top similarities for each record.')
    parser.add_argument('-l', '--min_norm_name_levenshtein_sim', default=0.7, type=float,
                        help="Minimum normalised Levenshtein similarity [0-1, 0=unequal, 1=equal].")
    args = parser.parse_args()

    delta_load_contactpersons(args)
