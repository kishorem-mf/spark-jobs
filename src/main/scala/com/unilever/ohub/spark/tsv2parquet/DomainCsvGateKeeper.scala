package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, Row}

import scala.reflect.runtime.universe._


abstract class DomainCsvGateKeeper[DomainType <: DomainEntity: TypeTag] extends DomainGateKeeper {

  override protected[t2v2parquet] def read(storage: Storage, input: String): Dataset[Row] = {
    storage
      .readFromCsv(
        location = input,
        fieldSeparator = fieldSeparator,
        hasHeaders = hasHeaders
      )
  }

}
