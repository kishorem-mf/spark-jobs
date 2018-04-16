package com.unilever.ohub.spark.tsv2parquet.file_interface

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.tsv2parquet.DomainCsvGateKeeper

trait FileDomainGateKeeper[T <: DomainEntity] extends DomainCsvGateKeeper[T] {
  override final val fieldSeparator: String = "‰"
  override final val hasHeaders: Boolean = true
  override final val partitionByValue = Seq("countryCode")
}
