package com.unilever.ohub.spark.export.acm

import java.io.{BufferedReader, File, InputStreamReader}
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.{Operator, TestContactPersons, TestOperators}
import com.unilever.ohub.spark.export.TargetType
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

  val storage = new InMemStorage(spark, operators, prevIntegrated)

  after {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(config.outboundLocation), true)
  }

  describe("ACM Operator csv generation") {
    it("should export only golden records and changed one") {
      val integratedDs = Seq(
        defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("3")),
        defaultOperator.copy(concatId = "AU~WUFOO~103", ohubId = Some("3"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"), isGoldenRecord = true)
      ).toDataset

      val prevIntegratedDs = Seq(
        defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("1")),
        defaultOperator.copy(concatId = "AU~WUFOO~103", ohubId = Some("2"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"))
      ).toDataset

      SUT.export(integratedDs, prevIntegratedDs, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 2
      result.filter($"DELETE_FLAG" === "N").collect().length shouldBe 2
    }

    it("should export only golden records and when OhubId has changed in new integrated mark it as deleted with targetOhubId") {
      val integratedDs = Seq(
        defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("3")),
        defaultOperator.copy(concatId = "AU~WUFOO~103", ohubId = Some("3")),
        defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("3"), isGoldenRecord = true)
      ).toDataset

      val prevIntegratedDs = Seq(
        defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("1")),
        defaultOperator.copy(concatId = "AU~WUFOO~103", ohubId = Some("2"), isGoldenRecord = true),
        defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"))
      ).toDataset

      SUT.export(integratedDs, prevIntegratedDs, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 2
      result.filter($"DELETE_FLAG" === "N").collect().length shouldBe 1
      result.filter($"DELETE_FLAG" === "Y").select($"TARGET_OHUB_ID").collect().head.toString() should include("3")
      result.filter($"DELETE_FLAG" === "N").collect() should have length 1
      result.filter($"DELETE_FLAG" === "N").select("OPR_LNKD_INTEGRATION_ID").collect().head.toString() should include("AU~104~1~19")
      result.filter($"DELETE_FLAG" === "Y").select("TARGET_OHUB_ID", "OPR_LNKD_INTEGRATION_ID").collect().head.mkString(":") should include("3:AU~103~1~19")
    }

  it("should export only golden records and when all ohubId has changed in integrated , then delete ones should be sent") {
    val integratedDs = Seq(
      defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("3")),
      defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("3")),
      defaultOperator.copy(concatId = "AU~WUFOO~103", ohubId = Some("3")),
      defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("3"), isGoldenRecord = true)
    ).toDataset

    val prevIntegratedDs = Seq(
      defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
      defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("1")),
      defaultOperator.copy(concatId = "AU~WUFOO~103", ohubId = Some("2"), isGoldenRecord = true),
      defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"))
    ).toDataset

    SUT.export(integratedDs, prevIntegratedDs, config, spark)

    val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

    result.collect().length shouldBe 3
    result.filter($"DELETE_FLAG" === "N").collect().length shouldBe 1
    result.filter($"DELETE_FLAG" === "Y").collect().length shouldBe 2
    result.filter($"DELETE_FLAG" === "N").select("OPR_LNKD_INTEGRATION_ID").collect().head.toString() should include("AU~104~1~19")
    result.filter($"OPR_ORIG_INTEGRATION_ID" === "2").select("TARGET_OHUB_ID","OPR_LNKD_INTEGRATION_ID", "DELETE_FLAG").collect().head.mkString(":") should include("3:AU~103~1~19:Y")
    result.filter($"OPR_ORIG_INTEGRATION_ID" === "1").select("TARGET_OHUB_ID","OPR_LNKD_INTEGRATION_ID", "DELETE_FLAG").collect().head.mkString(":") should include("3:AU~101~1~19:Y")
  }
}
}
