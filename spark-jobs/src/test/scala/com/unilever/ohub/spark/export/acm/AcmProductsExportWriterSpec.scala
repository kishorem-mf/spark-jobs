package com.unilever.ohub.spark.export.acm

import java.io.{BufferedReader, InputStreamReader}
import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.TestProducts
import com.unilever.ohub.spark.export.TargetType
import com.unilever.ohub.spark.export.domain.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{BeforeAndAfter, Matchers}

class AcmProductsExportWriterSpec
    extends SparkJobSpec
    with TestProducts
    with BeforeAndAfter
    with Matchers {

  import spark.implicits._

  val storage = new InMemStorage(spark, contactPersons, prevIntegrated)
  private val changedCP = defaultProduct.copy(
    isGoldenRecord = true,
    ohubId = Some("1234"),
    concatId = "AU~WUFOO~1234"
  )
  private val unchangedCP =
    defaultProduct.copy(isGoldenRecord = true, concatId = "AU~WUFOO~2345")
  private val SUT = com.unilever.ohub.spark.export.acm.ProductOutboundWriter
  private val contactPersons =
    Seq(changedCP, unchangedCP, defaultProduct).toDataset
  private val prevIntegrated = Seq(unchangedCP, defaultProduct).toDataset
  private val outboundLocation = UUID.randomUUID().toString
  private val config = export.OutboundConfig(
    integratedInputFile = "integrated",
    outboundLocation = outboundLocation,
    targetType = TargetType.ACM
  )

  after {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(config.outboundLocation), true)
  }

  describe("ACM csv generation") {

    it("Should contain header") {
      SUT.export(contactPersons, prevIntegrated, config, spark)

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val csvFile = fs
        .listStatus(new Path(config.outboundLocation))
        .find(status => status.getPath.getName.contains("UFS_PRODUCTS"))
        .get
      val header = readFirstLine(csvFile.getPath, fs)
      header shouldBe "COUNTY_CODE¶PRODUCT_NAME¶PRD_INTEGRATION_ID¶EAN_CODE¶MRDR_CODE¶CREATED_AT¶UPDATED_AT¶DELETE_FLAG"
    }

    it("Should write correct csv") {
      SUT.export(contactPersons, prevIntegrated, config, spark)

      val result = storage.readFromCsv(
        config.outboundLocation,
        new AcmOptions {}.delimiter,
        true
      )

      result.collect().length shouldBe 1
      result.collect().head.toString() should include("1234")
    }

    it(
      "should export only golden record, new record and changed record. Shouldn't export deleted records"
    ) {

      val prevIntegratedDs = Seq(
        defaultProduct.copy(ohubId = Some("3"), isGoldenRecord = true)
      ).toDataset

      val integratedDs = Seq(
        defaultProduct.copy(ohubId = Some("1"), isGoldenRecord = true),
        defaultProduct.copy(ohubId = Some("2"), isGoldenRecord = true),
        defaultProduct.copy(ohubId = Some("5"), isGoldenRecord = false)
      ).toDataset

      SUT.export(integratedDs, prevIntegratedDs, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(
        config.outboundLocation,
        new AcmOptions {}.delimiter,
        true
      )

      result.collect().length shouldBe 2
      result.filter($"DELETE_FLAG" === "N").collect().length shouldBe 2
      result
        .filter($"DELETE_FLAG" === "N")
        .select("PRD_INTEGRATION_ID")
        .collect()
        .mkString(":") should include("[1]:[2]")
    }
  }
  private def readFirstLine(path: Path, fs: FileSystem) = {
    val reader = new BufferedReader(
      new InputStreamReader(fs.open(path), "UTF-8")
    )
    reader.readLine()
  }
}
