package com.unilever.ohub.spark.export.ddl

import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.TestOperators
import com.unilever.ohub.spark.export.TargetType
import com.unilever.ohub.spark.export.domain.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, Matchers}

class OperatorDdlExportWriterSpec extends SparkJobSpec with TestOperators with BeforeAndAfter with Matchers {

  import spark.implicits._


  private val operators = Seq(defaultOperator).toDataset
  private val integrated = Seq(defaultOperator).toDataset
  private val SUT = com.unilever.ohub.spark.export.ddl.OperatorDdlOutboundWriter
  private val outboundLocation = UUID.randomUUID().toString
  private val config = export.OutboundConfig(
    integratedInputFile = "integrated",
    outboundLocation = outboundLocation,
    targetType = TargetType.DDL,
    auroraCountryCodes = "AU;country-code",
    fromDate = "2017-10-16 18:09:49",
    toDate = Some("2017-10-16 18:09:49"),
    sourceName = Some("FRONTIER")
  )

  val storage = new InMemStorage(spark, operators, integrated)

  after {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(config.outboundLocation), true)
  }

  describe("DDL Operator csv generation") {
    it("DDL Operator csv generation") {


      val integratedDs = Seq(
        defaultOperator.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true, sourceName = "FRONTIER"),
        defaultOperator.copy(concatId = "AU~WUFOO~102", ohubId = Some("3"), isGoldenRecord = true, sourceName = "FRONTIER"),
        defaultOperator.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"), isGoldenRecord = true, sourceName = "FRONTIER")
      ).toDataset

      SUT.exportToDdl(integratedDs, config, spark)

      val result = spark.read.option("sep",";").option("header","true").csv(config.outboundLocation + "/UFS_DDL_*.csv/*.csv")
      result.count() shouldBe 3
    }

  }

}
