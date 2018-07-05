package com.unilever.ohub.spark.ingest

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.{ DomainDataProvider, SparkJobSpec }
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.ingest.DomainGateKeeper.DomainConfig
import org.apache.spark.sql.Dataset

object CsvDomainGateKeeperSpec {
  type InputFile = String
}

trait CsvDomainGateKeeperSpec[DomainType <: DomainEntity] extends SparkJobSpec {
  import CsvDomainGateKeeperSpec._

  def SUT: CsvDomainGateKeeper[DomainType]
  def outputFile = ""

  def testDataProvider(): DomainDataProvider = TestDomainDataProvider()

  def runJobWith(inputFile: InputFile)(assertFn: Dataset[DomainType] ⇒ Unit): Unit =
    runJobWith(DomainConfig(inputFile, outputFile))(assertFn)

  def runJobWith(config: DomainConfig)(assertFn: Dataset[DomainType] ⇒ Unit): Unit = {
    val mockStorage = mock[Storage]

    // read input data
    (mockStorage.readFromCsv _).expects(config.inputFile, SUT.determineFieldSeparator(config), SUT.hasHeaders)
      .returns(
        spark
          .read
          .option("header", value = SUT.hasHeaders)
          .option("sep", SUT.determineFieldSeparator(config))
          .option("inferSchema", value = false)
          .csv(config.inputFile)
      )

    // write output data
    (mockStorage.writeToParquet(_: Dataset[DomainType], _: String, _: Seq[String])) expects where {
      (resultDataset, _, _) ⇒
        assertFn(resultDataset)
        true // let's not fail here, but do proper assertions in the assertion function
    }

    SUT.run(spark, config, mockStorage, testDataProvider())
  }
}
