package com.unilever.ohub.spark.ingest.sifu

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.{ DomainDataProvider, SparkJobSpec }
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.ingest.DomainGateKeeper.DomainConfig
import com.unilever.ohub.spark.ingest.TestDomainDataProvider
import org.apache.spark.sql.Dataset

trait SifuDomainGateKeeperDatasetSpec[DomainType <: DomainEntity] extends SparkJobSpec {

  def SUT: SifuDomainGateKeeper[DomainType]

  def inputFile = ""

  def outputFile = ""

  def testDataProvider(): DomainDataProvider = TestDomainDataProvider()

  def runJobWith()(assertFn: Dataset[DomainType] ⇒ Unit): Unit = {
    val mockStorage = mock[Storage]

    // write output data
    (mockStorage.writeToParquet(_: Dataset[DomainType], _: String, _: Seq[String])) expects where {
      (resultDataset, _, _) ⇒
        assertFn(resultDataset)
        true // let's not fail here, but do proper assertions in the assertion function
    }

    SUT.run(spark, DomainConfig(inputFile, outputFile), mockStorage, testDataProvider())
  }
}
