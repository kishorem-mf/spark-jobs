package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

import scala.reflect.runtime.universe._

abstract class DomainCsvGateKeeper[DomainType <: DomainEntity: TypeTag]
  extends DomainGateKeeper[DomainType, Row] {

  protected[tsv2parquet] def fieldSeparator: String

  protected[tsv2parquet] def hasHeaders: Boolean

  override protected def read(spark: SparkSession, storage: Storage, input: String): Dataset[Row] = {
    import spark.implicits._

    storage
      .readFromCsv(
        location = input,
        fieldSeparator = fieldSeparator,
        hasHeaders = hasHeaders
      )
  }

}
