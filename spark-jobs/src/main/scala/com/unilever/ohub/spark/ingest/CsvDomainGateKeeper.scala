package com.unilever.ohub.spark.ingest

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.ingest.DomainGateKeeper.DomainConfig
import org.apache.spark.sql.{ Dataset, Row, SparkSession }
import scopt.OptionParser

import scala.reflect.runtime.universe._

case class CsvDomainConfig(
    inputFile: String = "path-to-input-file",
    override val outputFile: String = "path-to-output-file",
    fieldSeparator: String = "field-separator",
    override val deduplicateOnConcatId: Boolean = true,
    override val strictIngestion: Boolean = true,
    override val showErrorSummary: Boolean = true
) extends DomainConfig

abstract class CsvDomainGateKeeper[DomainType <: DomainEntity: TypeTag] extends DomainGateKeeper[DomainType, Row, CsvDomainConfig] {

  protected def defaultFieldSeparator: String

  def hasHeaders: Boolean

  private[spark] def defaultConfig: CsvDomainConfig = CsvDomainConfig()

  private[spark] def configParser(): OptionParser[CsvDomainConfig] =
    new scopt.OptionParser[CsvDomainConfig]("Domain gate keeper") {
      head("converts a csv into domain entities and writes the result to parquet.", "1.0")
      opt[String]("inputFile") required () action { (x, c) ⇒
        c.copy(inputFile = x)
      } text "inputFile is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"
      opt[String]("fieldSeparator") optional () action { (x, c) ⇒
        c.copy(fieldSeparator = x)
      } text "fieldSeparator is a string property"
      opt[Boolean]("strictIngestion") optional () action { (x, c) ⇒
        c.copy(strictIngestion = x)
      } text "fieldSeparator is a string property"
      opt[Boolean]("deduplicateOnConcatId") optional () action { (x, c) ⇒
        c.copy(deduplicateOnConcatId = x)
      } text "deduplicateOnConcatId is a boolean property"
      opt[Boolean]("showErrorSummary") optional () action { (x, c) ⇒
        c.copy(showErrorSummary = x)
      } text "showErrorSummary is a boolean property"

      version("1.0")
      help("help") text "help text"
    }

  override protected def read(spark: SparkSession, storage: Storage, config: CsvDomainConfig): Dataset[Row] = {
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

  def determineFieldSeparator(config: CsvDomainConfig): String =
    if ("field-separator" == config.fieldSeparator) defaultFieldSeparator else config.fieldSeparator


}
