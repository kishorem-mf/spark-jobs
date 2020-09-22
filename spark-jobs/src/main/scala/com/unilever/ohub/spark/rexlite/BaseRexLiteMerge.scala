package com.unilever.ohub.spark.rexlite
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scopt.OptionParser

import scala.reflect.runtime.universe._

case class RexLiteMergeConfig (inputUrl: String = "input-file",
                               inputPrevious: String = "input-file-previous-integrated",
                               outputFile: String = "path-to-output-file",
                               prevIntegrated: String = "prev-integrated"
                              ) extends SparkJobConfig



abstract class BaseRexLiteMerge[T <: DomainEntity: TypeTag] extends SparkJob[RexLiteMergeConfig] {

  override private[spark] def defaultConfig = RexLiteMergeConfig()

  override private[spark] def configParser(): OptionParser[RexLiteMergeConfig] =
    new scopt.OptionParser[RexLiteMergeConfig]("Rex Lite Merge") {
      head("RexLiteMerge", "1.0")
      opt[String]("inputUrl") required() action { (x, c) ⇒
        c.copy(inputUrl = x)
      } text "inputFile is a string property"
      opt[String]("inputPrevious") required() action { (x, c) ⇒
        c.copy(inputPrevious = x)
      } text "inputPrevious is a string property"
      opt[String]("outputFile") required() action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"
      opt[String]("prevIntegrated") required() action { (x, c) ⇒
        c.copy(prevIntegrated = x)
      } text "prevIntegrated is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: RexLiteMergeConfig, storage: Storage): Unit = {
    import spark.implicits._

    val input_entity=storage.readFromParquet(config.inputUrl).toDF()
    val inputEntityPrevIntegrated=storage.readFromParquet(config.inputPrevious).toDF()
    val prevRexIntegrated=storage.readFromParquet[T](config.prevIntegrated)
    val input_entity_golden=storage.readFromParquet(config.inputUrl.replace(".parquet","_golden.parquet")).toDF()
    val input_delta=(input_entity.join(inputEntityPrevIntegrated,Seq("concatId"),JoinType.LeftAnti))
      .drop("additionalFields","ingestionErrors")

    val daily_merged_records:Dataset[T]=transform(spark,input_delta,input_entity_golden)

    val finalResult=prevRexIntegrated.unionByName(daily_merged_records.filter(col("rexLiteMergedDate")=!=""))
      .dropDuplicates()
      .as[T]

    storage.writeToParquet(finalResult, config.outputFile)
  }

  def transform(
                 spark: SparkSession,
                 input_entity: DataFrame,
                 input_entity_golden: DataFrame
               ): Dataset[T] = {
    import spark.implicits._

    val rexLiteEntity = input_entity
      .where(col("sourceName").===("FRONTIER"))
      .where(!col("countryCode").isin("RU", "US", "CA"))

    val mergedWithFrontierEntities = input_entity_golden
      .where(lower(col("sourceName")).like("%frontier%"))
      .where(length(col("sourceName")).>(length(lit("frontier"))))
    val mergedWithRexLiteEntities = mergedWithFrontierEntities
      .join(
        rexLiteEntity.select(col("ohubId").alias("rawOhubId")).distinct,
        col("ohubId").===(col("rawOhubId")),
        "inner"
      )
      .drop(col("rawOhubId"))
    val unionRecords = rexLiteEntity
      .withColumn("order", lit(1))
      .union(mergedWithRexLiteEntities.withColumn("order", lit(2)))
      .orderBy(col("ohubId"), col("concatId"))

    //Merge Partition by ohubId
    val entityWithListOfValues=mergeById(unionRecords)
    val latestFrontierInfoCopiedToAllFrontierRecordsWithSameOhubId=createDataFrameWithFirstValue(entityWithListOfValues,
      rexLiteEntity)
    val results=addMergeDate(rexLiteEntity,
      latestFrontierInfoCopiedToAllFrontierRecordsWithSameOhubId).as[T]
    results
  }
  def addMergeDate(rexLiteInitialData: DataFrame,latestTransformedData: DataFrame):DataFrame = {
    val before=rexLiteInitialData.drop("additionalFields","ingestionErrors")
    val columns: Array[String] = before.columns
    val after=latestTransformedData
      .select(columns.head, columns.tail: _*)

    val changedRecords=(after.exceptAll(before))
      .withColumn("rexLiteMergedDate",current_timestamp().as("current_timestamp"))
      .withColumn("additionalFields", typedLit(Map[String, String]()))
      .withColumn("ingestionErrors", typedLit(Map[String, IngestionError]()))
    val notChangedRecords=(after.intersect(before))
      .withColumn("rexLiteMergedDate",lit(""))
      .withColumn("additionalFields", typedLit(Map[String, String]()))
      .withColumn("ingestionErrors", typedLit(Map[String, IngestionError]()))

    val results=changedRecords.unionByName(notChangedRecords)
    results
  }

  def mergeById(unionRecords:DataFrame):DataFrame = {
    val ohubIdOrderDateWindow = Window
      .partitionBy("ohubId")
      .orderBy(
        col("order"),
        col("dateUpdated").desc_nulls_last,
        col("dateCreated").desc_nulls_last,
        col("ohubUpdated").desc_nulls_last
      )
    val partitionedRecords = unionRecords
      .withColumn("partitionerOld", row_number.over(ohubIdOrderDateWindow))
      .orderBy(col("ohubId"), col("partitionerOld").asc_nulls_last)
      .select(col("partitionerOld").alias("partitioner"), col("*"))
      .drop(col("partitionerOld"))

    val listExpressions = partitionedRecords
      .drop(col("partitioner"))
      .drop(col("ohubId"))
      .columns
      .map(column => collect_list(column).as(s"${column}List"))
    val entityWithListOfValues = partitionedRecords
      .drop(col("partitioner"))
      .groupBy(col("ohubId"))
      .agg(listExpressions.head, listExpressions.tail: _*)
    entityWithListOfValues
  }

  def createDataFrameWithFirstValue(entityWithListOfValues:DataFrame,rexLiteEntity:DataFrame):DataFrame = {
    val firstFromListExpressions = entityWithListOfValues
      .drop(col("ohubId"))
      .columns
      .map(list => col(list)(0).as(s"${list.replaceAll("List", "")}"))
    val finalRecords = entityWithListOfValues.select(
      Array(col("ohubId")).++(firstFromListExpressions): _*
    )

    val latestValues =
      rexLiteEntity
        .select(
          col("ohubId").alias("rexOhubId"),
          col("concatId").alias("rexConcatId"),
          col("sourceEntityId").alias("rexSourceEntityId")
        )
        .join(finalRecords, col("rexOhubId").===(col("ohubId")), "inner")
        .drop("rexOhubId", "concatId", "sourceEntityId")
        .withColumnRenamed("rexConcatId", "concatId")
        .withColumnRenamed("rexSourceEntityId", "sourceEntityId")
        .orderBy("ohubId")
    latestValues
  }

}
