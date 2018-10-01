package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

import scala.reflect.runtime.universe._

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
    val w = Window.partitionBy('concatId)
    val wAsc = w.orderBy('ohubCreated.asc)
    val wDesc = w.orderBy($"ohubCreated".desc)

    integratedDomainEntities
      .union(dailyDeltaDomainEntities)
      .withColumn("rn", row_number.over(wDesc))
      .withColumn("ohubCreated", first($"ohubCreated").over(wAsc))
      .filter('rn === 1)
      .drop('rn)
      .as[T]
  }

  override def run(spark: SparkSession, config: PreProcessConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Pre process delta domain entities with integrated [${config.integratedInputFile}] and delta" +
      s"[${config.deltaInputFile}] to pre processed delta output [${config.deltaPreProcessedOutputFile}]")

    val integratedDomainEntities = storage.readFromParquet[T](config.integratedInputFile)
    val dailyDeltaDomainEntities = storage.readFromParquet[T](config.deltaInputFile)

    val preProcessedDeltaDomainEntities = transform(spark, integratedDomainEntities, dailyDeltaDomainEntities)

    storage.writeToParquet(preProcessedDeltaDomainEntities, config.deltaPreProcessedOutputFile)
  }
}
