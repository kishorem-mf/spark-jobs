package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig, SparkJobWithDefaultConfig}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import scopt.OptionParser

import scala.reflect.runtime.universe._

case class ChangeLogConfig(
                            changeLogIntegrated: String = "change-log-integrated",
                            changeLogPrevious: String = "change-log-previous",
                            changeLogOutput: String = "path-to-output-file"
                          ) extends SparkJobConfig

abstract class BaseSCD[T <: DomainEntity: TypeTag] extends SparkJob[ChangeLogConfig] {

  override private[spark] def configParser(): OptionParser[ChangeLogConfig] =
    new scopt.OptionParser[ChangeLogConfig]("Change log") {
      head("change log entity output file.", "1.0")
      opt[String]("changeLogIntegrated") required () action { (x, c) ⇒
        c.copy(changeLogIntegrated = x)
      } text "changeLogIntegrated is a string property"
      opt[String]("changeLogPrevious") required () action { (x, c) ⇒
        c.copy(changeLogPrevious = x)
      } text "changeLogPrevious is a string property"
      opt[String]("changeLogOutput") required () action { (x, c) ⇒
        c.copy(changeLogOutput = x)
      } text "changeLogOutput is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override private[spark] def defaultConfig = ChangeLogConfig()

  def transform(spark: SparkSession, entity: Dataset[T], previousChangeLog: DataFrame): DataFrame = {
    import spark.implicits._

    val windowSpec = Window.partitionBy($"concatid").orderBy($"fromDate".desc)

    entity
      .filter(!$"ohubid".isNull)
      .withColumn("fileName", input_file_name())
      .withColumn("fileDate",regexp_extract(col("fileName"), """.*(\d{4}-\d{2}-\d{2}).*""", 1))
      .withColumn("fileDate", to_date($"fileDate", "yyyy-MM-dd"))
      .filter($"fileDate" =!= "")
      .filter($"fileDate" >= Timestamp.valueOf("2019-01-01 00:00:00.0")).groupBy("ohubid", "concatid")
      .agg(min("fileDate").alias("fromDate"))
      .withColumn("toDate", lit(null))  // scalastyle:ignore
      .unionByName(previousChangeLog)
      .groupBy("ohubid", "concatid")
      .agg(min("fromDate").alias("fromDate"))
      .withColumn("toDate", lag($"fromDate", 1) over windowSpec)
  }


}
