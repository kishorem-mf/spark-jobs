"""Tests for match_operators"""

import argparse
import glob
from os import path

from string_matching.entity_matching import preprocess_operators, main_operators


class TestMatchOperators:
    """Tests for match_operators"""

    def test_main(self, mocker, tmpdir, spark):
        """Test main_operators() with fake input arguments."""
        mocker.patch.object(
            argparse.ArgumentParser,
            "parse_args",
            return_value=argparse.Namespace(
                input_file=path.join(
                    path.dirname(__file__),
                    "data",
                    "operators",
                    "fake_operators_1K_empty_ohubId.snappy.parquet",
                ),
                output_path=str(tmpdir / "tmpoutput"),
                country_code="FAKE_COUNTRY",
                fraction="1.0",
                threshold=0,
                n_top=1500,
                min_norm_name_levenshtein_sim=0,
            ),
        )

        parser = argparse.ArgumentParser()
        main_operators(
            arguments=parser.parse_args(), preprocess_function=preprocess_operators
        )

        output_dir = str(tmpdir / "tmpoutput/countryCode=FAKE_COUNTRY")
        output_files = glob.glob(output_dir + "/*.parquet")
        assert len(output_files) >= 1

        output_df = spark.read.parquet(output_dir)
        assert output_df.count() >= 1
