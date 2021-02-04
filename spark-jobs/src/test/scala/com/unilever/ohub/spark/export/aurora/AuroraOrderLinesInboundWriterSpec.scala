package com.unilever.ohub.spark.export.aurora

import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.datalake.OrderLineOutboundWriter
import com.unilever.ohub.spark.domain.entity.{OrderLine, TestOrderLines}
import com.unilever.ohub.spark.export.TargetType
import com.unilever.ohub.spark.export.domain.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.spark.sql.Dataset


class AuroraOrderLinesInboundWriterSpec extends SparkJobSpec with TestOrderLines {

  import spark.implicits._

  private val SUT = OrderLineOutboundWriter
  private val outboundLocation = UUID.randomUUID().toString+"/"
  private val config = export.OutboundConfig(
    integratedInputFile = "raw",
    outboundLocation = outboundLocation,
    targetType = TargetType.UDL,
    //These are just random country codes used for testing as the input data for testing contains them. Changing them might need regression in other test cases
    auroraCountryCodes = "GB",
    fromDate = "2020-12-28"
  )

  describe("Aurora csv generation") {
    val ord: Dataset[OrderLine] = Seq(defaultOrderLine).toDataset
    // As InMemStorage has prevIntegrated as third parameter which is mandatory, we are passing the same integrated there
    val storage = new InMemStorage(spark, ord, ord)
    val inputFile = "src/test/resources/UDL_orderlines.csv"
    val result = SUT.transformInboundFilesByDate(inputFile,config.fromDate,config,spark,storage) //.run(spark, config, storage)

    it("Emakina source for sample orders") {
      val resultcsv = spark.read.option("sep",";").option("header","true").csv(config.outboundLocation + "EMAKINA/gb/orderlines/Processed/YYYY=2020/MM=12/DD=28/*.csv")
        resultcsv.filter($"orderType" === "Sample").count() shouldBe 1
    }

    it("Emakina source for webshop orders") {
      val resultcsv = spark.read.option("sep",";").option("header","true").csv(config.outboundLocation + "EMAKINA/gb/orderlines/Processed/YYYY=2020/MM=12/DD=28/*.csv")
      resultcsv.filter($"orderType" === "Webshop").count() shouldBe 0
    }

    it("Frontier source for orders") {
      val resultcsv = spark.read.option("sep",";").option("header","true").csv(config.outboundLocation + "FRONTIER/gb/orderlines/Processed/YYYY=2020/MM=12/DD=28/*.csv")
      resultcsv.count() shouldBe 4
    }

  }

}

