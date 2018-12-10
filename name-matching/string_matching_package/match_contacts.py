"""
Matching of contact persons based on name and location.

Only match contacts without e-mail AND without mobile phone number, because contacts are
already matched on this info.

The following steps are performed:
- Keep only contacts without e-mail AND without mobile phone number
- Remove contacts without first AND without last name (cleansed)
- Remove contacts without a street (cleansed)
- Create a matching-string: concatenation of first name and last name
- Per country
    - Match on matching-string
    - Keep only the matches with similarity above threshold
    - Keep only the matches with exactly matching zip code
        - If no zip code is present: keep match if cities (cleansed) match exactly
    - Keep only the matches where Levenshtein distance between streets (cleansed) is lower than threshold (5)
    - To generate a final list of matches, in the form of (i, j), i.e. contact i matches with contact j,
      we do the following:
        - Make sure each j only matches with one i (the 'group leader')
            - Note: of course we can have multiple matches per group leader, e.g. (i, j) and (i, k)
        - Make sure that each i (group leader) is not matched with another 'group leader',
                e.g. if we have (i, j) we remove (k, i) for all k
- Write Parquet file partitioned by country code
"""

import argparse

from string_matching.entity_matching import main_contactpersons

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--input_file',
                        help='fullpath or location of the input parquet file')
    parser.add_argument('-p', '--output_path', default=None,
                        help='write results in a parquet file to this fullpath or location directory')
    parser.add_argument('-c', '--country_code', default='all',
                        help='country code to use (e.g. US). Default all countries.')
    parser.add_argument('-frac', '--fraction', default=1.0, type=float,
                        help='use this fraction of records.')
    parser.add_argument('-t', '--threshold', default=0.9, type=float,
                        help='drop similarities below this value [0-1].')
    parser.add_argument('-n', '--n_top', default=1500, type=int,
                        help='keep N top similarities for each record.')
    parser.add_argument('-l', '--min_norm_name_levenshtein_sim', default=0.7, type=float,
                        help="Minimum normalised Levenshtein similarity [0-1, 0=unequal, 1=equal].")
    args = parser.parse_args()

    main_contactpersons(args)
