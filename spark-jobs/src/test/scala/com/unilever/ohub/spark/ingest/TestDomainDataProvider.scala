package com.unilever.ohub.spark.ingest

import com.unilever.ohub.spark.DomainDataProvider

case class TestDomainDataProvider(
    sourcePreferences: Map[String, Int] = Map(
      "WUFOO" -> 1,
      "EMAKINA" -> 2,
      "FUZZIT" -> 3,
      "SIFU" -> 4,
      "WEB_EVENT" -> 5,
      "KANGAROO" -> 6
    )
) extends DomainDataProvider
