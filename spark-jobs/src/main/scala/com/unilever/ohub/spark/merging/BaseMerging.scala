package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.SparkJobWithDefaultConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }
import org.apache.spark.sql.expressions.{ Window, WindowSpec }
import scala.reflect.runtime.universe._

abstract class BaseMerging[T <: DomainEntity: TypeTag] extends SparkJobWithDefaultConfig {
  val mergeGroupSizeCap = 100
  val prefixNewColumn = "merged_"
  val excludeFields = Seq("group_row_num")

  def transform(spark: SparkSession, ds: Dataset[T]): Dataset[T] = {
    import spark.implicits._

    val groupWindow = Window.partitionBy($"ohubId")

    val orderByDatesWindow = groupWindow.orderBy(
      when($"dateUpdated".isNull, $"dateCreated").otherwise($"dateUpdated").desc_nulls_last,
      $"dateCreated".desc_nulls_last,
      $"ohubUpdated".desc
    )

    val mergeableRecords = ds
      .filter($"isActive")
      .withColumn("group_row_num", row_number().over(orderByDatesWindow))
      .filter($"group_row_num" <= mergeGroupSizeCap)
      .withColumn("sourceName", concat_ws(",", sort_array(collect_set("sourceName").over(groupWindow))))
      .drop("additionalFields", "ingestionErrors")

    setFieldsToLatestValue(
      spark,
      orderByDatesWindow,
      mergeableRecords,
      excludeFields = excludeFields,
      reversedOrderColumns = Seq("dateCreated", "ohubCreated")
    )
      .filter($"group_row_num" === 1)
      .drop("group_row_num")
      .withColumn("isGoldenRecord", lit(true))
      .withColumn("additionalFields", typedLit(Map[String, String]()))
      .withColumn("ingestionErrors", typedLit(Map[String, IngestionError]()))
      .as[T]
  }

  private[merging] def pickOldest(spark: SparkSession, df: DataFrame, column: String): DataFrame = {
    import spark.implicits._

    // Picks the oldest per record and writes to a new column
    val groupWindowForCreatedDates = Window.partitionBy($"ohubId")

    val orderByCreatedDateWindow = groupWindowForCreatedDates.orderBy(col(column).asc_nulls_last)

    df.withColumn(
      prefixNewColumn + column, first(col(column), true).over(
        orderByCreatedDateWindow.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
      )
    )
  }

  private[merging] def pickNewest(spark: SparkSession, df: DataFrame, column: String, newestNotNullWindow: WindowSpec): DataFrame = {
    // Picks the newest per record and writes to a new column

    df.withColumn(
      prefixNewColumn + column, first(col(column), true).over(
        newestNotNullWindow.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
    )
  }

  private[merging] def setFieldsToLatestValue(
    spark: SparkSession, orderByDatesWindow: WindowSpec,
    dataframe: DataFrame, excludeFields: Seq[String] = Seq(),
    reversedOrderColumns: Seq[String] = Seq()
  ): DataFrame = {
    // Set all columns of dataset on the first value of it's newestNotNullWindow
    // Note: we write the result to a new column as prefix+columnName because overwriting introduces randomness

    val columns = dataframe.columns.filter(!excludeFields.contains(_))
    val columnsModified = columns.map(column ⇒ prefixNewColumn + column)

    var dataframeModified: DataFrame = columns
      .foldLeft(dataframe)(
        (op: DataFrame, column: String) ⇒ {

          if (reversedOrderColumns.contains(column)) {
            pickOldest(spark, op, column)
          } else {
            pickNewest(spark, op, column, orderByDatesWindow)
          }
        }

      )

    dataframeModified
      .select(columnsModified.map(c ⇒ col(c)): _*)
      .toDF(columns: _*)
  }
}
