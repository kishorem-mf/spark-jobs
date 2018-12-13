package com.unilever.ohub.spark.outbound

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession._
import com.unilever.ohub.spark.domain.{ DomainEntity, DomainEntityHash }
import com.unilever.ohub.spark.domain.entity.TestContactPersons
import com.unilever.ohub.spark.storage.DefaultStorage
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.h2.tools.Server

class ContactPersonOutboundWriterSpec extends SparkJobSpec with TestContactPersons {
  import spark.implicits._

  private val SUT = ContactPersonOutboundWriter
  private val contactpersons = Seq(defaultContactPerson).toDataset
  private val hashes = Seq[DomainEntityHash](
    DomainEntityHash(defaultContactPerson.concatId, Some(true), Some("some-hash"))
  ).toDataset

  val h2Server: Server = Server.createTcpServer().start

  describe("ContactPersonOutboundWriter") {
    it("writes to rdbms correctly") {
      val config = OutboundConfig(
        integratedInputFile = "integrated",
        dbUrl = "jdbc:h2:tcp://localhost/~/test",
        dbTable = "contactpersons",
        dbUsername = "sa",
        dbPassword = ""
      )
      val storage = new InMemStorage(spark, contactpersons, hashes)

      SUT.run(spark, config, storage)

      val expectedResult = SUT.snakeColumns(contactpersons.drop("additionalFields", "ingestionErrors").withColumn("hasChanged", lit(true)))
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

class InMemStorage[DomainType <: DomainEntity](spark: SparkSession, entities: Dataset[DomainType], hashes: Dataset[DomainEntityHash]) extends DefaultStorage(spark) {
  import scala.reflect.runtime.universe._

  override def readFromParquet[T <: Product: TypeTag](location: String, selectColumns: Seq[Column] = Seq()): Dataset[T] = {
    implicit val encoder = Encoders.product[T]

    if (location == "integrated") {
      entities.as[T]
    } else {
      hashes.as[T]
    }
  }

  override def readJdbcTable(dbUrl: String, dbTable: String, userName: String, userPassword: String, jdbcDriverClass: String): DataFrame = {
    super.readJdbcTable(dbUrl, dbTable, userName, userPassword, "org.h2.Driver")
  }

  override def writeJdbcTable(df: DataFrame, dbUrl: String, dbTable: String, userName: String, userPassword: String,
    jdbcDriverClass: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    super.writeJdbcTable(df, dbUrl, dbTable, userName, userPassword, "org.h2.Driver")
  }
}
