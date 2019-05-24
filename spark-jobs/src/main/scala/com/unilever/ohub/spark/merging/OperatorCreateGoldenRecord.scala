package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{DefaultConfig, SparkJobWithDefaultConfig}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object OperatorCreateGoldenRecord extends SparkJobWithDefaultConfig {
  def transform(spark: SparkSession, operators: Dataset[Operator]): Dataset[Operator] = {
    import spark.implicits._
    val groupWindow = Window.partitionBy($"ohubId")
    val activeOperators = operators.filter($"isActive").drop("additionalFields", "ingestionErrors")

    setFieldsToLatestValue(spark, groupWindow, activeOperators)
      .filter($"group_row_num" === 1)
      .drop("group_row_num")
      .withColumn("isGoldenRecord", lit(true))
      .withColumn("additionalFields", typedLit(Map[String, String]()))
      .withColumn("ingestionErrors", typedLit(Map[String, IngestionError]()))
      .as[Operator]
  }

  private[merging] def setFieldsToLatestValue(spark: SparkSession, groupWindow: WindowSpec, dataframe: DataFrame, excludeFields: Seq[String] = Seq()): DataFrame = {
    import spark.implicits._

    val newestNotNullWindow = groupWindow.orderBy(
      $"dateUpdated".desc_nulls_last,
      $"dateCreated".desc_nulls_last,
      $"ohubUpdated".desc
    )

    // Set all columns of dataset on the first value of it's newestNotNullWindow
    dataframe.columns
      .filter(!excludeFields.contains(_))
      .foldLeft(dataframe)(
        (op: DataFrame, column: String) ⇒ {
          op.withColumn(column, first(col(column), true).over(newestNotNullWindow))
        }
      )
      .withColumn("group_row_num", row_number().over(newestNotNullWindow))
  }

  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {
    log.info(s"Creating golden operators records based on [${config.inputFile}] and writing them to [${config.outputFile}]")

    val entities = storage.readFromParquet[Operator](config.inputFile)

    val transformed = transform(spark, entities)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
