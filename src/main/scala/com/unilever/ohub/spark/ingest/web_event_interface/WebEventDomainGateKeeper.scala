package com.unilever.ohub.spark.ingest.web_event_interface

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeper

trait WebEventDomainGateKeeper[T <: DomainEntity] extends CsvDomainGateKeeper[T] {
  override final val defaultFieldSeparator = ";"
  override final val hasHeaders = true
  override final val partitionByValue = Seq()
  final val SourceName = "EMAKINA"
}
