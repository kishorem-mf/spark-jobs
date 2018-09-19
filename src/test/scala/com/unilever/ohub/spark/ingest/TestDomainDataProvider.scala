package com.unilever.ohub.spark.ingest

import com.unilever.ohub.spark.DomainDataProvider
import com.unilever.ohub.spark.data.{ ChannelMapping, CountryRecord, CountrySalesOrg }
import org.apache.spark.sql.Dataset

case class TestDomainDataProvider(
    countries: Map[String, CountryRecord] = Map(
      "AU" -> CountryRecord("AU", "Australia", "AUD"),
      "DE" -> CountryRecord("DE", "Germany", "EUR"),
      "NL" -> CountryRecord("NL", "Netherlands", "EUR"),
      "NZ" -> CountryRecord("NZ", "New Zealand", "???"),
      "TR" -> CountryRecord("TR", "Turkey", "Turkish lira")
    ),
    sourcePreferences: Map[String, Int] = Map(
      "WUFOO" -> 1,
      "EMAKINA" -> 2,
      "FUZZIT" -> 3,
      "SIFU" -> 4,
      "WEB_EVENT" -> 5,
      "KANGAROO" -> 6
    ),
    countrySalesOrg: Map[String, CountrySalesOrg] = Map(
      "1030" -> CountrySalesOrg("NL", Some("1030")),
      "1611" -> CountrySalesOrg("DE", Some("1611"))
    )
) extends DomainDataProvider {
  override def channelMappings(): Dataset[ChannelMapping] = ???
}
