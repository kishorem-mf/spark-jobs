package com.unilever.ohub.spark.outbound

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import org.apache.spark.sql.{ SparkSession, DataFrame }
import scopt.OptionParser

import scala.reflect.runtime.universe._

case class OutboundConfig(
    integratedInputFile: String = "integrated-input-file",
    dbUrl: String = "db-url",
    dbTable: String = "db-table",
    dbUsername: String = "db-username",
    dbPassword: String = "db-password"
) extends SparkJobConfig

abstract class DomainOutboundWriter[DomainType <: DomainEntity: TypeTag] extends SparkJob[OutboundConfig] {

  override private[spark] def defaultConfig = OutboundConfig()

  override private[spark] def configParser(): OptionParser[OutboundConfig] =
    new scopt.OptionParser[OutboundConfig]("Spark job default") {
      head("run a spark job with default config.", "1.0")

      opt[String]("integratedInputFile") required () action { (x, c) ⇒
        c.copy(integratedInputFile = x)
      } text "integratedInputFile is a string property"
      opt[String]("dbUrl") required () action { (x, c) ⇒
        c.copy(dbUrl = x)
      } text "dbUrl is a string property"
      opt[String]("dbTable") required () action { (x, c) ⇒
        c.copy(dbTable = x)
      } text "dbTable is a string property"
      opt[String]("dbUsername") required () action { (x, c) ⇒
        c.copy(dbUsername = x)
      } text "dbUsername is a string property"
      opt[String]("dbPassword") required () action { (x, c) ⇒
        c.copy(dbPassword = x)
      } text "dbPassword is a string property"

      version("1.0")
      help("help") text "help text"
    }

  def camelToSnake(str: String): String = {
    val snake = "([A-Z])".r.replaceAllIn(str, m ⇒ "_" + m.group(1).toLowerCase)
    "(^_)|(_$)".r.replaceAllIn(snake, _ ⇒ "")
  }

  def snakeColumns(df: DataFrame): DataFrame = {
    df.toDF(df.columns.toSeq.map(camelToSnake): _*)
  }

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    log.info(s"writing integrated entities [${config.integratedInputFile}] to outbound db in table [${config.dbTable}].")

    val integratedEntities = storage.readFromParquet[DomainType](config.integratedInputFile)
      .drop("additionalFields", "ingestionErrors")

    storage.writeJdbcTable(snakeColumns(integratedEntities), config.dbUrl, config.dbTable, config.dbUsername, config.dbPassword)
  }
}
