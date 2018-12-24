package com.unilever.ohub.spark.outbound

import com.unilever.ohub.spark.domain.{ DomainEntity, DomainEntityHash }
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity._
import com.unilever.ohub.spark.sql.JoinType
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions.when
import scopt.OptionParser

import scala.reflect.runtime.universe._

object OperatorOutboundWriter extends DomainOutboundWriter[Operator]

object ContactPersonOutboundWriter extends DomainOutboundWriter[ContactPerson]

object SubscriptionOutboundWriter extends DomainOutboundWriter[Subscription]

object ProductOutboundWriter extends DomainOutboundWriter[Product]

object OrderOutboundWriter extends DomainOutboundWriter[Order]

object OrderLineOutboundWriter extends DomainOutboundWriter[OrderLine]

object ActivityOutboundWriter extends DomainOutboundWriter[Activity]

object QuestionOutboundWriter extends DomainOutboundWriter[Question]

object AnswerOutboundWriter extends DomainOutboundWriter[Answer]

object CampaignOutboundWriter extends DomainOutboundWriter[Campaign]

object CampaignBounceOutboundWriter extends DomainOutboundWriter[CampaignBounce]

object CampaignClickOutboundWriter extends DomainOutboundWriter[CampaignClick]

object CampaignOpenOutboundWriter extends DomainOutboundWriter[CampaignOpen]

object CampaignSendOutboundWriter extends DomainOutboundWriter[CampaignSend]

case class OutboundConfig(
    integratedInputFile: String = "integrated-input-file",
    hashesInputFile: Option[String] = None,
    numberOfPartitions: Int = 200,
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
      opt[String]("hashesInputFile") optional () action { (x, c) ⇒
        c.copy(hashesInputFile = Some(x))
      } text "hashesInputFile is a string property"
      opt[Int]("numberOfPartitions") optional () action { (x, c) ⇒
        c.copy(numberOfPartitions = x)
      } text "numberOfPartitions is an integer property"
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
    import spark.implicits._

    log.info(s"writing integrated entities [${config.integratedInputFile}] to outbound db in table [${config.dbTable}].")

    val hashesInputFile = config.hashesInputFile match {
      case Some(location) ⇒ storage.readFromParquet[DomainEntityHash](location)
      case None           ⇒ spark.createDataset(Seq[DomainEntityHash]()) // no hash file configured -> everything marked as changed
    }
    val integratedEntities = storage.readFromParquet[DomainType](config.integratedInputFile)
    val columnsInOrder = integratedEntities.drop("additionalFields", "ingestionErrors").columns :+ "hasChanged"
    val result = integratedEntities
      .join(hashesInputFile, Seq("concatId"), JoinType.LeftOuter)
      .withColumn("hasChanged", when('hasChanged.isNull, true).otherwise('hasChanged)) // defaults to changed
      .select(columnsInOrder.head, columnsInOrder.tail: _*) // preserve original order of columns
      .coalesce(config.numberOfPartitions)

    storage.writeJdbcTable(snakeColumns(result), config.dbUrl, config.dbTable, config.dbUsername, config.dbPassword)
  }
}
