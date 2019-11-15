package com.unilever.ohub.spark.export.dispatch

import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.TestOperators
import com.unilever.ohub.spark.export.TargetType
import com.unilever.ohub.spark.export.dispatch.OperatorOutboundWriter.commonTransform
import com.unilever.ohub.spark.export.domain.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{BeforeAndAfter, Matchers}

class DispatcherOperatorsExportWriterSpec extends SparkJobSpec with TestOperators with BeforeAndAfter with Matchers {

  import spark.implicits._

  private val operators = Seq(defaultOperator).toDataset
  private val prevIntegrated = Seq(defaultOperator).toDataset
  private val SUT = com.unilever.ohub.spark.export.dispatch.OperatorOutboundWriter
  private val outboundLocation = UUID.randomUUID().toString
  private val config = export.OutboundConfig(
    integratedInputFile = "integrated",
    outboundLocation = outboundLocation,
    targetType = TargetType.DISPATCHER
  )

  val emptyDF = spark.emptyDataFrame

  val storage = new InMemStorage(spark, operators, prevIntegrated)

  after {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(config.outboundLocation), true)
  }

  describe("Dispatcher Operator csv generation") {
    it("Should write correct csv with MPGR and integrated Operators") {

      val prevIntegratedDs = Seq(
        defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("1")),
        defaultOperator.copy(concatId = "AU~WUFOO~103", ohubId = Some("2"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"))
      ).toDataset

      val integratedDs = Seq(
        defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("3")),
        defaultOperator.copy(concatId = "AU~WUFOO~103", ohubId = Some("3"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"), isGoldenRecord = true)
      ).toDataset

      val previousMergedDs = Seq(
        defaultOperator.copy(ohubId = Some("1"), concatId = "AU~OHUB~1", isGoldenRecord = true),
        defaultOperator.copy(ohubId = Some("2"), concatId = "AU~OHUB~2", isGoldenRecord = true)
      ).toDataset

      val mergedDs = Seq(
        defaultOperator.copy(ohubId = Some("1"), concatId = "AU~OHUB~1", isGoldenRecord = true),
        defaultOperator.copy(ohubId = Some("2"), concatId = "AU~OHUB~2", isGoldenRecord = true),
        defaultOperator.copy(ohubId = Some("3"), concatId = "AU~OHUB~3", isGoldenRecord = true)
      ).toDataset

      val deletedIntegrated = commonTransform(integratedDs, prevIntegratedDs, config, spark).map(_.copy(isGoldenRecord = false))
      SUT.export(integratedDs, deletedIntegrated.unionByName, mergedDs, previousMergedDs, emptyDF, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new DispatcherOptions {}.delimiter, true)
      
      result.count shouldBe 4
      result.filter($"GOLDEN_RECORD_FLAG" === "Y").collect().length shouldBe 1
      result.filter($"GOLDEN_RECORD_FLAG" === "Y").select("OPR_ORIG_INTEGRATION_ID").collect().headOption.map(_.getString(0)) shouldBe Some("AU~OHUB~3")
    }

  }

}
