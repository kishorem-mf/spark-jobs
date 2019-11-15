package com.unilever.ohub.spark.export.acm

import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.TestOperators
import com.unilever.ohub.spark.export.TargetType
import com.unilever.ohub.spark.export.acm.OperatorOutboundWriter.getDeletedOhubIdsWithTargetId
import com.unilever.ohub.spark.export.domain.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{BeforeAndAfter, Matchers}

class AcmOperatorsExportWriterSpec extends SparkJobSpec with TestOperators with BeforeAndAfter with Matchers {

  import spark.implicits._

  private val operators = Seq(defaultOperator).toDataset
  private val prevIntegrated = Seq(defaultOperator).toDataset
  private val SUT = com.unilever.ohub.spark.export.acm.OperatorOutboundWriter
  private val outboundLocation = UUID.randomUUID().toString
  private val config = export.OutboundConfig(
    integratedInputFile = "integrated",
    outboundLocation = outboundLocation,
    targetType = TargetType.ACM
  )
  private val emptyDF = spark.emptyDataFrame

  val storage = new InMemStorage(spark, operators, prevIntegrated)

  after {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(config.outboundLocation), true)
  }

  describe("ACM Operator csv generation") {
    it("should export only new merged ohubids to ACM when there are no deleted records required") {

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

      SUT.export(integratedDs, getDeletedOhubIdsWithTargetId(spark, prevIntegratedDs, integratedDs, previousMergedDs, mergedDs).unionByName, mergedDs, previousMergedDs, emptyDF, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 1
      result.filter($"DELETE_FLAG" === "N").collect().length shouldBe 1
      result.filter($"DELETE_FLAG" === "N").select("OPR_ORIG_INTEGRATION_ID").collect().mkString(":") should include("[3]")
    }

    it("should export only golden records and when OhubId has changed in new integrated mark it as deleted with targetOhubId") {

      val prevIntegratedDs = Seq(
        defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("1")),
        defaultOperator.copy(concatId = "AU~WUFOO~103", ohubId = Some("2"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"))
      ).toDataset

      val integratedDs = Seq(
        defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("3")),
        defaultOperator.copy(concatId = "AU~WUFOO~103", ohubId = Some("3")),
        defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("3"), isGoldenRecord = true)
      ).toDataset

      val previousMergedDs = Seq(
        defaultOperator.copy(ohubId = Some("1"), concatId = "AU~OHUB~1", isGoldenRecord = true),
        defaultOperator.copy(ohubId = Some("2"), concatId = "AU~OHUB~2", isGoldenRecord = true)
      ).toDataset

      val mergedDs = Seq(
        defaultOperator.copy(ohubId = Some("1"), concatId = "AU~OHUB~1", isGoldenRecord = true),
        defaultOperator.copy(ohubId = Some("3"), concatId = "AU~OHUB~3", isGoldenRecord = true)
      ).toDataset

      SUT.export(integratedDs, getDeletedOhubIdsWithTargetId(spark, prevIntegratedDs, integratedDs, previousMergedDs, mergedDs).unionByName, mergedDs, previousMergedDs, emptyDF, config, spark)

      val result = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 2
      result.filter($"DELETE_FLAG" === "N").collect().length shouldBe 1
      result.filter($"DELETE_FLAG" === "N").select("OPR_ORIG_INTEGRATION_ID").collect().head.toString() should include("3")
      result.filter($"DELETE_FLAG" === "Y").collect().length shouldBe 1

      /* We hav e one inconsintency, for deleted records and their concatid.
       * Because we look at the integrated for determining the deleted records and target groups, the concatid also comes from the integrated
       */
      result.filter($"DELETE_FLAG" === "Y").select("TARGET_OHUB_ID", "OPR_LNKD_INTEGRATION_ID").collect().head.mkString(":") should include("3:AU~103~1~19")
    }

    it("should export only golden records and when all ohubId has changed in integrated , then delete ones should be sent") {

      val prevIntegratedDs = Seq(
        defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("1")),
        defaultOperator.copy(concatId = "AU~WUFOO~103", ohubId = Some("2"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"))
      ).toDataset

      val integratedDs = Seq(
        defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("3")),
        defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("3")),
        defaultOperator.copy(concatId = "AU~WUFOO~103", ohubId = Some("3")),
        defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("3"), isGoldenRecord = true)
      ).toDataset

      val previousMergedDs = Seq(
        defaultOperator.copy(ohubId = Some("1"), concatId = "AU~OHUB~1", isGoldenRecord = true),
        defaultOperator.copy(ohubId = Some("2"), concatId = "AU~OHUB~2", isGoldenRecord = true)
      ).toDataset

      val mergedDs = Seq(
        defaultOperator.copy(ohubId = Some("3"), concatId = "AU~OHUB~3", isGoldenRecord = true)
      ).toDataset

      SUT.export(integratedDs, getDeletedOhubIdsWithTargetId(spark, prevIntegratedDs, integratedDs, previousMergedDs, mergedDs).unionByName, mergedDs, previousMergedDs, emptyDF, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 3
      result.filter($"DELETE_FLAG" === "N").collect().length shouldBe 1
      result.filter($"DELETE_FLAG" === "Y").collect().length shouldBe 2
      result.filter($"DELETE_FLAG" === "N").select("OPR_ORIG_INTEGRATION_ID").collect().head.toString() should include("3")

      /* We have one inconsistency for deleted records and their concatid.
       * Because we look at the integrated for determining the deleted records and target groups, the concatid also comes from the integrated files
       */
      result.filter($"OPR_ORIG_INTEGRATION_ID" === "2").select("TARGET_OHUB_ID", "OPR_LNKD_INTEGRATION_ID", "DELETE_FLAG").collect().head.mkString(":") should include("3:AU~103~1~19:Y")
      result.filter($"OPR_ORIG_INTEGRATION_ID" === "1").select("TARGET_OHUB_ID", "OPR_LNKD_INTEGRATION_ID", "DELETE_FLAG").collect().head.mkString(":") should include("3:AU~101~1~19:Y")
    }

    it("should send DE-ACTIVATED groups as deleted") {

      val prevIntegratedDs = Seq(
        defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true, isActive = true),
        defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("1"), isActive = true),
        defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"), isGoldenRecord = true, isActive = true),
        defaultOperator.copy(concatId = "AU~WUFOO~105", ohubId = Some("2"), isGoldenRecord = true, isActive = true)
      ).toDataset

      val integratedDs = Seq(
        defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true, isActive = false),
        defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("1"), isActive = false),
        defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"), isGoldenRecord = true, isActive = true),
        defaultOperator.copy(concatId = "AU~WUFOO~105", ohubId = Some("2"), isGoldenRecord = false, isActive = false)
      ).toDataset

      val previousMergedDs = Seq(
        defaultOperator.copy(ohubId = Some("1"), concatId = "AU~OHUB~1", isGoldenRecord = true, isActive = true),
        defaultOperator.copy(ohubId = Some("2"), concatId = "AU~OHUB~2", isGoldenRecord = true, isActive = true)
      ).toDataset

      val mergedDs = Seq(
        defaultOperator.copy(ohubId = Some("2"), concatId = "AU~OHUB~2", isGoldenRecord = true, isActive = true)
      ).toDataset

      SUT.export(integratedDs, getDeletedOhubIdsWithTargetId(spark, prevIntegratedDs, integratedDs, previousMergedDs, mergedDs).unionByName, mergedDs, previousMergedDs, emptyDF, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 1
      result.filter($"DELETE_FLAG" === "N").collect().length shouldBe 0
      result.filter($"DELETE_FLAG" === "Y").collect().length shouldBe 1
      result.filter($"OPR_ORIG_INTEGRATION_ID" === "1").select("TARGET_OHUB_ID").first.getString(0) should equal(null)
      result.filter($"OPR_ORIG_INTEGRATION_ID" === "1").select("DELETE_FLAG").first.getString(0) should equal("Y")
    }

    it("should send changed groups and DE-ACTIVATED without interfering with each other") {

      val prevIntegratedDs = Seq(
        defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("1")),
        defaultOperator.copy(concatId = "AU~WUFOO~103", ohubId = Some("2"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("2")),
        defaultOperator.copy(concatId = "AU~WUFOO~901", ohubId = Some("5"), isGoldenRecord = true, isActive = true),
        defaultOperator.copy(concatId = "AU~WUFOO~902", ohubId = Some("5"), isActive = true)
      ).toDataset

      val integratedDs = Seq(
        defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("3")),
        defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("3")),
        defaultOperator.copy(concatId = "AU~WUFOO~103", ohubId = Some("3")),
        defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("3"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~901", ohubId = Some("5"), isGoldenRecord = true, isActive = false),
        defaultOperator.copy(concatId = "AU~WUFOO~902", ohubId = Some("5"), isActive = false)
      ).toDataset

      val previousMergedDs = Seq(
        defaultOperator.copy(ohubId = Some("1"), concatId = "AU~OHUB~1", isGoldenRecord = true),
        defaultOperator.copy(ohubId = Some("2"), concatId = "AU~OHUB~2", isGoldenRecord = true),
        defaultOperator.copy(ohubId = Some("5"), concatId = "AU~OHUB~5", isGoldenRecord = true)
      ).toDataset

      val mergedDs = Seq(
        defaultOperator.copy(ohubId = Some("3"), concatId = "AU~OHUB~3", isGoldenRecord = true)
      ).toDataset

      SUT.export(integratedDs, getDeletedOhubIdsWithTargetId(spark, prevIntegratedDs, integratedDs, previousMergedDs, mergedDs).unionByName, mergedDs, previousMergedDs, emptyDF, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 4
      result.filter($"DELETE_FLAG" === "N").collect().length shouldBe 1
      result.filter($"DELETE_FLAG" === "Y").collect().length shouldBe 3
      result.filter($"DELETE_FLAG" === "N").select("OPR_ORIG_INTEGRATION_ID").collect().head.toString() should include("3")

      result.filter($"OPR_ORIG_INTEGRATION_ID" === "2").select("TARGET_OHUB_ID", "OPR_LNKD_INTEGRATION_ID", "DELETE_FLAG").collect().head.mkString(":") should include("3:AU~103~1~19:Y")
      result.filter($"OPR_ORIG_INTEGRATION_ID" === "1").select("TARGET_OHUB_ID", "OPR_LNKD_INTEGRATION_ID", "DELETE_FLAG").collect().head.mkString(":") should include("3:AU~101~1~19:Y")
      result.filter($"OPR_ORIG_INTEGRATION_ID" === "5").select("TARGET_OHUB_ID").first.getString(0) should equal(null)
      result.filter($"OPR_ORIG_INTEGRATION_ID" === "5").select("DELETE_FLAG").first.getString(0) should equal("Y")
    }
  }

}
