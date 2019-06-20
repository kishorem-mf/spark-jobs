package com.unilever.ohub.spark.anonymization

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class AnonymizationMergingConfig(inputFile: String = "input-file",
                                      previousIntegrated: String = "previous-integrated-file",
                                      outputFile: String = "path-to-output-file"
                                     ) extends SparkJobConfig

object AnonymizationMerging extends SparkJob[AnonymizationMergingConfig] {

  def transform(newIdentifiers: Dataset[AnonymizedContactPersonIdentifier],
                previousIntegrated: Dataset[AnonymizedContactPersonIdentifier]
               ): Dataset[AnonymizedContactPersonIdentifier] = {
    previousIntegrated.union(newIdentifiers).dropDuplicates()
  }

  override private[spark] def defaultConfig = AnonymizationMergingConfig()

  override private[spark] def configParser(): OptionParser[AnonymizationMergingConfig] =
    new scopt.OptionParser[AnonymizationMergingConfig]("Anonymized merging") {
      head("merges anonymized into an integrated anonymized output file.", "1.0")
      opt[String]("inputFile") required() action { (x, c) ⇒
        c.copy(inputFile = x)
      } text "inputFile is a string property"
      opt[String]("previousIntegrated") required() action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required() action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: AnonymizationMergingConfig, storage: Storage): Unit = {
    import spark.implicits._

    val newAnonymizedContactPersons = spark
      .read
      .option("header", true)
      .csv(config.inputFile)
      .as[AnonymizedContactPersonIdentifier]

    var previousIntegrated = storage.readFromParquet[AnonymizedContactPersonIdentifier](config.previousIntegrated)
    if (previousIntegrated.head(1).isEmpty) {
      previousIntegrated = spark.createDataset[AnonymizedContactPersonIdentifier](Seq[AnonymizedContactPersonIdentifier]())
    }

    storage.writeToParquet(transform(newAnonymizedContactPersons, previousIntegrated), config.outputFile)
  }
}
