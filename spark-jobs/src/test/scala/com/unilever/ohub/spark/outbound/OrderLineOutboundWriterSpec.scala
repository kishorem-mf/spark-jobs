package com.unilever.ohub.spark.outbound

import com.unilever.ohub.spark.SharedSparkSession._
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.DomainEntityHash
import com.unilever.ohub.spark.domain.entity.TestOrderLines
import org.apache.spark.sql.functions._
import org.h2.tools.Server

class OrderLineOutboundWriterSpec extends SparkJobSpec with TestOrderLines {
  import spark.implicits._

  private val SUT = OrderLineOutboundWriter
  private val orderLines = Seq(defaultOrderLine, orderLineWithOrderTypeSSD, orderLineWithOrderTypeTRANSFER).toDataset
  private val hashes = Seq[DomainEntityHash](
    DomainEntityHash(defaultOrderLine.concatId, Some(true), Some("some-hash"))
  ).toDataset

  val h2Server: Server = Server.createTcpServer().start

  describe("OrderLineOutboundWriter") {
    it("writes to rdbms correctly") {
      val config = OutboundConfig(
        integratedInputFile = "integrated",
        dbUrl = "jdbc:h2:tcp://localhost/~/test",
        dbTable = "orders",
        dbUsername = "sa",
        dbPassword = ""
      )
      val storage = new InMemStorage(spark, orderLines, hashes)

      SUT.run(spark, config, storage)

      val expectedResult = SUT.snakeColumns(Seq(defaultOrderLine).toDataset.drop("additionalFields", "ingestionErrors").withColumn("hasChanged", lit(true)))
      val actualResult = storage.readJdbcTable(
        config.dbUrl,
        config.dbTable,
        config.dbUsername,
        config.dbPassword
      )

      actualResult.columns shouldBe expectedResult.columns
      actualResult.collect() shouldBe expectedResult.collect()
    }
  }
}

