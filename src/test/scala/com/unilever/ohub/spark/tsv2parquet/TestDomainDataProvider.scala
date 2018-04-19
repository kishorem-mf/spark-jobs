package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.data.{ CountryRecord, CountrySalesOrg }

case class TestDomainDataProvider(
    countries: Map[String, CountryRecord] = Map(
      "AU" -> CountryRecord("AU", "Australia", "AUD"),
      "DE" -> CountryRecord("DE", "Germany", "EUR"),
      "NL" -> CountryRecord("NL", "Netherlands", "EUR")
    ),
    sourcePreferences: Map[String, Int] = Map(
      "WUFOO" -> 1,
      "EMAKINA" -> 2,
      "FUZZIT" -> 3,
      "SIFU" -> 4
    ),
    countrySalesOrg: Map[String, CountrySalesOrg] = Map(
      "1030" -> CountrySalesOrg("NL", Some("1030")),
      "1611" -> CountrySalesOrg("DE", Some("1611"))
    )
) extends DomainDataProvider {
}
