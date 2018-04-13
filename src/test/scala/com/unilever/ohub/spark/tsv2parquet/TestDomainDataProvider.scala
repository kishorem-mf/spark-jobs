package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.data.CountryRecord

case class TestDomainDataProvider(
    countries: Map[String, CountryRecord] = Map(),
    sourcePreferences: Map[String, Int] = Map(
      "WUFOO" -> 1,
      "EMAKINA" -> 2,
      "FUZZIT" -> 3
    )
) extends DomainDataProvider
