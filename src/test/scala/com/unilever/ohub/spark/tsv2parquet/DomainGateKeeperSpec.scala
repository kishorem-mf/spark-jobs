package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.Dataset

object DomainGateKeeperSpec {
  type InputFile = String
}

trait DomainGateKeeperSpec[DomainType <: DomainEntity] extends SparkJobSpec {
  import DomainGateKeeperSpec._

  private[tsv2parquet] val SUT: DomainCsvGateKeeper[DomainType]
  private[tsv2parquet] val outputFile = ""

  def testDataProvider(): DomainDataProvider = TestDomainDataProvider()

  def runJobWith(inputFile: InputFile)(assertFn: Dataset[DomainType] ⇒ Unit): Unit = {
    val mockStorage = mock[Storage]

    // read input data
    (mockStorage.readFromCsv _).expects(inputFile, SUT.fieldSeparator, SUT.hasHeaders)
      .returns(
        spark
          .read
          .option("header", value = SUT.hasHeaders)
          .option("sep", SUT.fieldSeparator)
          .option("inferSchema", value = false)
          .csv(inputFile)
      )

    // write output data
    (mockStorage.writeToParquet(_: Dataset[DomainType], _: String, _: Seq[String])) expects where {
      (resultDataset, _, _) ⇒
        assertFn(resultDataset)
        true // let's not fail here, but do proper assertions in the assertion function
    }

    SUT.run(spark, (inputFile, outputFile), mockStorage, testDataProvider())
  }
}
