""" Matching of contact persons based on name and location.

Only match contacts without e-mail AND without mobile phone number,
because contacts are already matched on this info.

The following steps are performed:
- keep only contacts without e-mail AND without mobile phone number
- remove contacts without first AND without last name (cleansed)
- remove contacts without a street (cleansed)
- create a unique ID as COUNTRY_CODE~SOURCE~REF_CONTACT_PERSON_ID
- create a matching-string: concatenation of first name and last name
- per country
    - match on matching-string
    - keep only the matches with similarity above threshold (0.75)
    - keep only the matches with exactly matching zip code
        - if no zip code is present: keep match if cities (cleansed) match exactly
    - keep only the matches where Levenshtein distance between streets (cleansed) is lower than threshold (5)
    - to generate a final list of matches, in the form of (i, j), i.e. contact i matches with contact j,
      we do the following:
        - make sure each j only matches with one i (the 'group leader')
            - note: of course we can have multiple matches per group leader, e.g. (i, j) and (i, k)
        - make sure that each i (group leader) is not matched with another 'group leader',
        e.g. if we have (i, j) we remove (k, i) for all k
- write parquet file partitioned by country code
"""

import argparse

from string_matching.initial_load_matching import \
    main, \
    preprocess_contacts, match_contacts_for_country

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-f', '--input_file',
                        help='fullpath or location of the input parquet file')
    parser.add_argument('-p', '--output_path', default=None,
                        help='write results in a parquet file to this fullpath or location directory')
    parser.add_argument('-c', '--country_code', default='all',
                        help='country code to use (e.g. US). Default all countries.')
    parser.add_argument('-frac', '--fraction', default=1.0, type=float,
                        help='use this fraction of records.')
    parser.add_argument('-t', '--threshold', default=0.75, type=float,
                        help='drop similarities below this value [0-1].')
    parser.add_argument('-n', '--n_top', default=1500, type=int,
                        help='keep N top similarities for each record.')
    args = parser.parse_args()

    main(args, preprocess_contacts, match_contacts_for_country)
