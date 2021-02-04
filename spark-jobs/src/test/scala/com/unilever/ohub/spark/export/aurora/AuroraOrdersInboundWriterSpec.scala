package com.unilever.ohub.spark.export.aurora

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.datalake.OrderOutboundWriter
import com.unilever.ohub.spark.domain.entity.{Order, TestOrders}
import com.unilever.ohub.spark.export.TargetType
import com.unilever.ohub.spark.export.domain.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.spark.sql.Dataset





class AuroraOrdersInboundWriterSpec extends SparkJobSpec with TestOrders {

  import spark.implicits._

  private val SUT = OrderOutboundWriter
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
    val ord: Dataset[Order] = Seq(defaultOrder).toDataset
    // As InMemStorage has prevIntegrated as third parameter which is mandatory, we are passing the same integrated there
    val storage = new InMemStorage(spark, ord, ord)
    val inputFile = "src/test/resources/UDL_orders.csv"
    val result = SUT.transformInboundFilesByDate(inputFile,config.fromDate,config,spark,storage) //.run(spark, config, storage)

    it("Emakina source for sample orders") {
      val resultcsv = spark.read.option("sep",";").option("header","true").csv(config.outboundLocation + "EMAKINA/gb/orders/Processed/YYYY=2020/MM=12/DD=28/*.csv")
        resultcsv.filter($"orderType" === "Sample").count() shouldBe 1
    }

    it("Emakina source for webshop orders") {
      val resultcsv = spark.read.option("sep",";").option("header","true").csv(config.outboundLocation + "EMAKINA/gb/orders/Processed/YYYY=2020/MM=12/DD=28/*.csv")
      resultcsv.filter($"orderType" === "Webshop").count() shouldBe 0
    }

    it("Frontier source for orders") {
      val resultcsv = spark.read.option("sep",";").option("header","true").csv(config.outboundLocation + "FRONTIER/gb/orders/Processed/YYYY=2020/MM=12/DD=28/*.csv")
      resultcsv.count() shouldBe 4
    }

  }

}

