package com.unilever.ohub.spark.outbound

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession._
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.entity.TestContactPersons
import com.unilever.ohub.spark.storage.DefaultStorage
import org.apache.spark.sql._
import org.h2.tools.Server

class ContactPersonOutboundWriterSpec extends SparkJobSpec with TestContactPersons {
  import spark.implicits._

  private val SUT = ContactPersonOutboundWriter
  private val contactpersons = Seq(defaultContactPerson).toDataset

  val h2Server: Server = Server.createTcpServer().start

  describe("ContactPersonOutboundWriter") {
    it("writes to rdbms correctly") {
      val config = OutboundConfig(
        dbUrl = "jdbc:h2:tcp://localhost/~/test",
        dbTable = "contactpersons",
        dbUsername = "sa",
        dbPassword = ""
      )
      val storage = new InMemStorage(spark, contactpersons)

      SUT.run(spark, config, storage)

      val expectedResult = SUT.snakeColumns(contactpersons.drop("additionalFields", "ingestionErrors"))
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

class InMemStorage[DomainType <: DomainEntity](spark: SparkSession, entities: Dataset[DomainType]) extends DefaultStorage(spark) {
  import scala.reflect.runtime.universe._

  override def readFromParquet[T <: Product: TypeTag](location: String, selectColumns: Seq[Column] = Seq()): Dataset[T] = {
    implicit val encoder = Encoders.product[T]

    entities.as[T]
  }

  override def readJdbcTable(dbUrl: String, dbTable: String, userName: String, userPassword: String, jdbcDriverClass: String): DataFrame = {
    super.readJdbcTable(dbUrl, dbTable, userName, userPassword, "org.h2.Driver")
  }

  override def writeJdbcTable(df: DataFrame, dbUrl: String, dbTable: String, userName: String, userPassword: String,
    jdbcDriverClass: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    super.writeJdbcTable(df, dbUrl, dbTable, userName, userPassword, "org.h2.Driver")
  }
}
