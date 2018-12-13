"""Tests for delta_operators"""

import argparse
import glob
from os import path

from string_matching.entity_delta_matching import delta_load_operators
from string_matching.entity_matching import preprocess_operators


class TestDeltaOperators:
    """Tests for delta_operators"""

    def test_main(self, mocker, tmpdir, spark):
        """Test main_operators() with fake input arguments."""
        operators_testdata_path = path.join(path.dirname(__file__), "data", "operators")
        mocker.patch.object(
            argparse.ArgumentParser,
            "parse_args",
            return_value=argparse.Namespace(
                integrated_input_path=path.join(
                    operators_testdata_path,
                    "fake_operators_1K_with_ohubId.snappy.parquet",
                ),
                ingested_daily_input_path=path.join(
                    operators_testdata_path,
                    "fake_operators_100_empty_ohubId.snappy.parquet",
                ),
                updated_integrated_output_path=str(tmpdir / "integrated"),
                unmatched_output_path=str(tmpdir / "unmatched"),
                country_code="FAKE_COUNTRY",
                fraction="1.0",
                threshold=0,
                n_top=1500,
                min_norm_name_levenshtein_sim=0,
            ),
        )

        parser = argparse.ArgumentParser()
        delta_load_operators(parser.parse_args())

        integrated_output_dir = str(tmpdir / "integrated/countryCode=FAKE_COUNTRY")
        unmatched_output_dir = str(tmpdir / "unmatched/countryCode=FAKE_COUNTRY")

        assert len(glob.glob(integrated_output_dir + "/*.parquet")) >= 1
        assert len(glob.glob(unmatched_output_dir + "/*.parquet")) >= 1

        integrated = spark.read.parquet(integrated_output_dir)
        unmatched = spark.read.parquet(unmatched_output_dir)
        assert integrated.count() + unmatched.count() >= 1
