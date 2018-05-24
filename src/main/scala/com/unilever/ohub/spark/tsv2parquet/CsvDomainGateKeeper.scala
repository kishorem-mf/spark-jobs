package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeper.DomainConfig
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

import scala.reflect.runtime.universe._

abstract class CsvDomainGateKeeper[DomainType <: DomainEntity: TypeTag] extends DomainGateKeeper[DomainType, Row] {

  protected[tsv2parquet] def defaultFieldSeparator: String

  protected[tsv2parquet] def hasHeaders: Boolean

  override protected def read(spark: SparkSession, storage: Storage, config: DomainConfig): Dataset[Row] = {
    val fieldSeparator = determineFieldSeparator(config)
    val result = storage
      .readFromCsv(
        location = config.inputFile,
        fieldSeparator = fieldSeparator,
        hasHeaders = hasHeaders
      )

    val numberOfFields = result.schema.fields.length
    if (numberOfFields == 1) {
      log.error(s"Number of fields in schema is '$numberOfFields', probably the fieldSeparator is specified incorrectly, currently it's '$fieldSeparator', which resulted in the following schema: ${result.schema}.")
      System.exit(1)
    }

    result
  }

  protected[tsv2parquet] def determineFieldSeparator(config: DomainConfig): String =
    if ("field-separator" == config.fieldSeparator) defaultFieldSeparator else config.fieldSeparator
}
