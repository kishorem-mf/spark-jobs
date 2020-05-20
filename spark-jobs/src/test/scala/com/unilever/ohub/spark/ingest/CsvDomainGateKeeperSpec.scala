package com.unilever.ohub.spark.ingest

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SaveMode }

object CsvDomainGateKeeperSpec {
  type InputFile = String
}

trait CsvDomainGateKeeperSpec[DomainType <: DomainEntity] extends SparkJobSpec {
  import CsvDomainGateKeeperSpec._

  def SUT: CsvDomainGateKeeper[DomainType] // scalastyle:ignore
  def outputFile:String = ""

  def runJobWith(inputFile: InputFile)(assertFn: Dataset[DomainType] ⇒ Unit): Unit =
    runJobWith(CsvDomainConfig(inputFile, outputFile))(assertFn)

  def runJobWith(config: CsvDomainConfig)(assertFn: Dataset[DomainType] ⇒ Unit): Unit = {
    val mockStorage = mock[Storage]

    val escapeChar = "\""
    // read input data
    (mockStorage.readFromCsv _).expects(config.inputFile, SUT.determineFieldSeparator(config), SUT.hasHeaders, escapeChar)
      .returns(
        spark
          .read
          .option("header", value = SUT.hasHeaders)
          .option("sep", SUT.determineFieldSeparator(config))
          .option("inferSchema", value = false)
          .csv(config.inputFile)
      )

    // write output data
    (mockStorage.writeToParquet(_: Dataset[DomainType], _: String, _: Seq[String], _: SaveMode)) expects where {
      (resultDataset, _, _, _) ⇒
        assertFn(resultDataset)
        true // let's not fail here, but do proper assertions in the assertion function
    }

    SUT.run(spark, config, mockStorage)
  }
}
