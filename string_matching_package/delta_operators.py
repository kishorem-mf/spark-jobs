""" Join ingested daily data with integrated data, keeping persistent group id's

This script outputs two dataframes:
- Updated integrated data
  - Changed records that match with integrated data
  - New records that match with integrated data
- Unmatched data
  - All records that do not match with integrated data

The following steps are performed per country:
- Pre-process integrated data to format for joining
- Pre-process ingested daily data to format for joining
- Match ingested data with integrated data
- write two dataframes to file: updated integrated data and unmatched data
"""
import argparse
from string_matching.entity_matching import preprocess_operators
from string_matching.entity_delta_matching import main, postprocess_delta_operators

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--integrated_operators_input_path',
                        help='full path or location of the parquet file with integrated operator data')
    parser.add_argument('-g', '--ingested_daily_operators_input_path',
                        help='full path or location of the parquet file with ingested daily operator data')

    parser.add_argument('-p', '--updated_integrated_output_path', default=None,
                        help='write results in a parquet file to this full path or location directory')
    parser.add_argument('-q', '--unmatched_output_path', default=None,
                        help='write results in a parquet file to this full path or location directory')

    parser.add_argument('-c', '--country_code', default='all',
                        help='country code to use (e.g. US). Default all countries.')
    parser.add_argument('-t', '--threshold', default=0.8, type=float,
                        help='drop similarities below this value [0.-1.].')
    parser.add_argument('-n', '--n_top', default=1500, type=int,
                        help='keep N top similarities for each record.')
    args = parser.parse_args()

    main(args, preprocess_operators, postprocess_delta_operators)
