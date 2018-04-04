package com.unilever.ohub.spark.tsv2parquet.file_interface

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeper

trait FileDomainGateKeeper[T <: DomainEntity] extends DomainGateKeeper[T] {
  override final val fieldSeparator: String = "‰"
  override final val hasHeaders: Boolean = true
  override final val partitionByValue = Seq("countryCode")
}
