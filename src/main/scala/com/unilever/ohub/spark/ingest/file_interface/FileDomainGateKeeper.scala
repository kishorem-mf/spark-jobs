package com.unilever.ohub.spark.ingest.file_interface

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeper

trait FileDomainGateKeeper[T <: DomainEntity] extends CsvDomainGateKeeper[T] {
  override final val defaultFieldSeparator: String = ";"
  override final val hasHeaders: Boolean = true
  override final val partitionByValue = Seq()
}
