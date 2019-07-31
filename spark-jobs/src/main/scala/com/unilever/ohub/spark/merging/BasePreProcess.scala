package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.entity._
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

import scala.reflect.runtime.universe._

object OperatorPreProcess extends BasePreProcess[Operator]

object ContactPersonPreProcess extends BasePreProcess[ContactPerson]

object SubscriptionPreProcess extends BasePreProcess[Subscription]

object OrderPreProcess extends BasePreProcess[Order]

object OrderLinePreProcess extends BasePreProcess[OrderLine]

object ProductPreProcess extends BasePreProcess[Product]

object RecipePreProcess extends BasePreProcess[Recipe]

object ActivityPreProcess extends BasePreProcess[Activity]

object AnswerPreProcess extends BasePreProcess[Answer]

object QuestionPreProcess extends BasePreProcess[Question]

object LoyaltyPointsPreProcess extends BasePreProcess[LoyaltyPoints]

object CampaignPreProcess extends BasePreProcess[Campaign]

object CampaignBouncePreProcess extends BasePreProcess[CampaignBounce]

object CampaignOpenPreProcess extends BasePreProcess[CampaignOpen]

object CampaignSendPreProcess extends BasePreProcess[CampaignSend]

object CampaignClickPreProcess extends BasePreProcess[CampaignClick]

object ChannelMappingPreProcess extends BasePreProcess[ChannelMapping]

object ChainPreProcess extends BasePreProcess[Chain]

case class PreProcessConfig(
    integratedInputFile: String = "path-to-integrated-input-file",
    deltaInputFile: String = "path-to-delta-input-file",
    deltaPreProcessedOutputFile: String = "path-to-delta-pre-processed-output-file"
) extends SparkJobConfig

abstract class BasePreProcess[T <: DomainEntity: TypeTag] extends SparkJob[PreProcessConfig] {

  override private[spark] def defaultConfig = PreProcessConfig()

  override private[spark] def configParser(): OptionParser[PreProcessConfig] =
    new scopt.OptionParser[PreProcessConfig]("Spark job default") {
      head("run a spark job with default config.", "1.0")
      opt[String]("integratedInputFile") required () action { (x, c) ⇒
        c.copy(integratedInputFile = x)
      } text "integratedInputFile is a string property"
      opt[String]("deltaInputFile") required () action { (x, c) ⇒
        c.copy(deltaInputFile = x)
      } text "deltaInputFile is a string property"
      opt[String]("deltaPreProcessedOutputFile") required () action { (x, c) ⇒
        c.copy(deltaPreProcessedOutputFile = x)
      } text "deltaPreProcessedOutputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  def transform(spark: SparkSession, integratedDomainEntities: Dataset[T], dailyDeltaDomainEntities: Dataset[T]): Dataset[T] = {
    import spark.implicits._

    // pre-process domain entities...for each updated domain entity...set the correct ohubCreated
    val w = Window.partitionBy('concatId).orderBy('ohubCreated.asc)

    val integrated = integratedDomainEntities.withColumn("inDelta", lit(false))
    val delta = dailyDeltaDomainEntities.withColumn("inDelta", lit(true))

    integrated
      .union(delta)
      .withColumn("ohubCreated", first('ohubCreated).over(w))
      .filter('inDelta === true)
      .drop('inDelta)
      .as[T]
  }

  override def run(spark: SparkSession, config: PreProcessConfig, storage: Storage): Unit = {
    log.info(s"Pre process delta domain entities with integrated [${config.integratedInputFile}] and delta" +
      s"[${config.deltaInputFile}] to pre processed delta output [${config.deltaPreProcessedOutputFile}]")

    val integratedDomainEntities = storage.readFromParquet[T](config.integratedInputFile)
    val dailyDeltaDomainEntities = storage.readFromParquet[T](config.deltaInputFile)

    val preProcessedDeltaDomainEntities = transform(spark, integratedDomainEntities, dailyDeltaDomainEntities)

    storage.writeToParquet(preProcessedDeltaDomainEntities, config.deltaPreProcessedOutputFile)
  }
}
