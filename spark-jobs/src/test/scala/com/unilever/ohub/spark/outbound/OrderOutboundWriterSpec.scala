package com.unilever.ohub.spark.outbound

import com.unilever.ohub.spark.SharedSparkSession._
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.DomainEntityHash
import com.unilever.ohub.spark.domain.entity.TestOrders
import org.apache.spark.sql.functions._
import org.h2.tools.Server

class OrderOutboundWriterSpec extends SparkJobSpec with TestOrders {
  import spark.implicits._

  private val SUT = OrderOutboundWriter
  private val orders = Seq(defaultOrder, orderWithOrderTypeSSD, orderWithOrderTypeTRANSFER).toDataset
  private val hashes = Seq[DomainEntityHash](
    DomainEntityHash(defaultOrder.concatId, Some(true), Some("some-hash"))
  ).toDataset

  val h2Server: Server = Server.createTcpServer().start

  describe("OrderOutboundWriter") {
    it("writes to rdbms correctly") {
      val config = OutboundConfig(
        integratedInputFile = "integrated",
        dbUrl = "jdbc:h2:tcp://localhost/~/test",
        dbTable = "orders",
        dbUsername = "sa",
        dbPassword = ""
      )
      val storage = new InMemStorage(spark, orders, hashes)

      SUT.run(spark, config, storage)

      val expectedResult = SUT.snakeColumns(Seq(defaultOrder).toDataset.drop("additionalFields", "ingestionErrors").withColumn("hasChanged", lit(true)))
      val actualResult = storage.readJdbcTable(
        config.dbUrl,
        config.dbTable,
        config.dbUsername,
        config.dbPassword
      )

      actualResult.columns shouldBe expectedResult.columns
      actualResult.collect() shouldBe expectedResult.collect()
    }

    it("creates snake columns correctly") {
      SUT.camelToSnake("someConcatId") shouldBe "some_concat_id"
    }
  }
}

