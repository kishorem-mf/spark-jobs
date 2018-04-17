package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.sifu.SifuProductResponse
import org.apache.spark.sql.Dataset

trait DomainGateKeeperDatasetSpec[DomainType <: DomainEntity] extends SparkJobSpec {
  private[tsv2parquet] val SUT: DomainGateKeeper[DomainType, SifuProductResponse]
  private[tsv2parquet] val inputFile = ""
  private[tsv2parquet] val outputFile = ""

  def testDataProvider(): DomainDataProvider = TestDomainDataProvider()

  def runJobWith(response: Seq[SifuProductResponse])(assertFn: Dataset[DomainType] ⇒ Unit): Unit = {
    val mockStorage = mock[Storage]

    (mockStorage.productsFromApi _).expects("NL", "nl", "products", 1, 1)
      .returns(response)

    // write output data
    (mockStorage.writeToParquet(_: Dataset[DomainType], _: String, _: Seq[String])) expects where {
      (resultDataset, _, _) ⇒
        assertFn(resultDataset)
        true // let's not fail here, but do proper assertions in the assertion function
    }

    SUT.run(spark, (inputFile, outputFile), mockStorage, testDataProvider())
  }
}
