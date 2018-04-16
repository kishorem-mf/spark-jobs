package com.unilever.ohub.spark.tsv2parquet.emakina

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeper

trait EmakinaDomainGateKeeper[T <: DomainEntity] extends DomainGateKeeper[T] {
  override final val fieldSeparator = ";"
  override final val hasHeaders = true
  override final val partitionByValue = Seq("countryCode")
  val sourceName = "EMAKINA"
}
