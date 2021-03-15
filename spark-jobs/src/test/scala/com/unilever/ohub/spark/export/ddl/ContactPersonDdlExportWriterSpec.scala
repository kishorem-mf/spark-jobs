package com.unilever.ohub.spark.export.ddl

import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.TestContactPersonsGolden
import com.unilever.ohub.spark.export.TargetType
import com.unilever.ohub.spark.export.domain.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, Matchers}

class ContactPersonDdlExportWriterSpec extends SparkJobSpec with TestContactPersonsGolden with BeforeAndAfter with Matchers {

  import spark.implicits._


  private val contactpersons = Seq(defaultContactPersonGolden).toDataset
  private val integrated = Seq(defaultContactPersonGolden).toDataset
  private val SUT = com.unilever.ohub.spark.export.ddl.ContactPersonDdlOutboundWriter
  private val outboundLocation = UUID.randomUUID().toString
  private val config = export.OutboundConfig(
    integratedInputFile = "integrated",
    outboundLocation = outboundLocation,
    targetType = TargetType.DDL,
    auroraCountryCodes = "AU;country-code",
    fromDate = "2015-06-29 18:09:49",
    toDate = Some("2015-07-01 18:09:49"),
    sourceName = "FRONTIER"
  )

  val storage = new InMemStorage(spark, contactpersons, integrated)

  after {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(config.outboundLocation), true)
  }

  describe("DDL Contact Person csv generation") {
    it("DDL Contact Person csv generation") {


      val integratedDs = Seq(
        defaultContactPersonGolden.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), sourceName = "FRONTIER", isGoldenRecord = true),
        defaultContactPersonGolden.copy(concatId = "AU~WUFOO~102", ohubId = Some("3"), sourceName = "FRONTIER", isGoldenRecord = true),
        defaultContactPersonGolden.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"), sourceName = "FRONTIER", isGoldenRecord = true)
      ).toDataset

      SUT.exportToDdl(integratedDs, config, spark)

      val result = spark.read.option("sep", ";").option("header", "true").csv(config.outboundLocation + "/AFH_SALESFORCE_*.csv")
      result.count() shouldBe 3
    }
  }
}
