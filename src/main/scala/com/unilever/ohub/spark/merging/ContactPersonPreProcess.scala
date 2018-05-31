package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class ContactPersonPreProcessConfig(
    integratedInputFile: String = "path-to-integrated-input-file",
    deltaInputFile: String = "path-to-delta-input-file",
    deltaPreProcessedOutputFile: String = "path-to-delta-pre-processed-output-file"
) extends SparkJobConfig

object ContactPersonPreProcess extends SparkJob[ContactPersonPreProcessConfig] {

  override private[spark] def defaultConfig = ContactPersonPreProcessConfig()

  override private[spark] def configParser(): OptionParser[ContactPersonPreProcessConfig] =
    new scopt.OptionParser[ContactPersonPreProcessConfig]("Spark job default") {
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

  def transform(spark: SparkSession, integratedContactPersons: Dataset[ContactPerson], dailyDeltaContactPersons: Dataset[ContactPerson]): Dataset[ContactPerson] = {
    import spark.implicits._

    val contactPersonCombined = integratedContactPersons
      .union(dailyDeltaContactPersons)
      .toDF()

    // pre-process contact persons...for each updated contact person...set the correct ohubCreated
    val w = Window.partitionBy('concatId)
    val wAsc = w.orderBy('ohubCreated.asc)
    val wDesc = w.orderBy($"ohubCreated".desc)

    contactPersonCombined
      .withColumn("rn", row_number.over(wDesc))
      .withColumn("ohubCreated", first($"ohubCreated").over(wAsc))
      .filter('rn === 1)
      .drop('rn)
      .as[ContactPerson]

  }

  override def run(spark: SparkSession, config: ContactPersonPreProcessConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Pre process delta contact persons with integrated [${config.integratedInputFile}] and delta" +
      s"[${config.deltaInputFile}] to pre processed delta output [${config.deltaPreProcessedOutputFile}]")

    val integratedContactPersons = storage.readFromParquet[ContactPerson](config.integratedInputFile)
    val dailyDeltaContactPersons = storage.readFromParquet[ContactPerson](config.deltaInputFile)

    val preProcessedDeltaContactPersons = transform(spark, integratedContactPersons, dailyDeltaContactPersons)

    storage.writeToParquet(preProcessedDeltaContactPersons, config.deltaPreProcessedOutputFile)
  }
}
