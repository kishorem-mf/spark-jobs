package com.unilever.ohub.spark.tsv2parquet.emakina

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.tsv2parquet.CsvDomainGateKeeper

trait EmakinaDomainGateKeeper[T <: DomainEntity] extends CsvDomainGateKeeper[T] {
  override final val defaultFieldSeparator = ";"
  override final val hasHeaders = true
  override final val partitionByValue = Seq()
  val sourceName = "EMAKINA"
}
