package com.unilever.ohub.spark.tsv2parquet.file_interface

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.tsv2parquet.CsvDomainGateKeeper

trait FileDomainGateKeeper[T <: DomainEntity] extends CsvDomainGateKeeper[T] {
  override final val defaultFieldSeparator: String = ";"
  override final val hasHeaders: Boolean = true
  override final val partitionByValue = Seq("countryCode")
}
