package com.unilever.ohub.spark.tsv2parquet.emakina

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.tsv2parquet.DomainCsvGateKeeper
import org.apache.spark.sql.Row

trait EmakinaDomainGateKeeper[T <: DomainEntity] extends DomainCsvGateKeeper[T] {
  override final val fieldSeparator = ";"
  override final val hasHeaders = true
  override final val partitionByValue = Seq("countryCode")
}
