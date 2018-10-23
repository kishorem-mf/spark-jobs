package com.unilever.ohub.spark.ingest.sifu

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.{ DomainDataProvider, SparkJobSpec }
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.ingest.TestDomainDataProvider
import org.apache.spark.sql.{ Dataset, SaveMode }

trait SifuDomainGateKeeperDatasetSpec[DomainType <: DomainEntity] extends SparkJobSpec {

  def SUT: SifuDomainGateKeeper[DomainType]

  def outputFile = ""

  def testDataProvider(): DomainDataProvider = TestDomainDataProvider()

  def runJobWith()(assertFn: Dataset[DomainType] ⇒ Unit): Unit = {
    val mockStorage = mock[Storage]

    // write output data
    (mockStorage.writeToParquet(_: Dataset[DomainType], _: String, _: Seq[String], _: SaveMode)) expects where {
      (resultDataset, _, _, _) ⇒
        assertFn(resultDataset)
        true // let's not fail here, but do proper assertions in the assertion function
    }

    SUT.run(spark, SifuDomainConfig(outputFile), mockStorage, testDataProvider())
  }
}
