package com.unilever.ohub.spark.tsv2parquet.web_event

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.tsv2parquet.CsvDomainGateKeeper

trait WebEventDomainGateKeeper[T <: DomainEntity] extends CsvDomainGateKeeper[T] {
  override final val fieldSeparator = ";"
  override final val hasHeaders = true
  override final val partitionByValue = Seq("countryCode")
  val sourceName = "WEB_EVENT" // TODO: change? update enums to add this?
}
