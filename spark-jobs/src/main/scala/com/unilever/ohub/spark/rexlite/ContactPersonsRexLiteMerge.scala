package com.unilever.ohub.spark.rexlite

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.entity.{ContactPerson, ContactPersonGolden, ContactPersonRexLite}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, typedLit}

object ContactPersonsRexLiteMerge extends BaseRexLiteMerge[ContactPersonRexLite] {
  override def run(spark: SparkSession, config: RexLiteMergeConfig, storage: Storage): Unit = {
    import spark.implicits._
    val input_entity=storage.readFromParquet[ContactPerson](config.inputUrl).toDF()
    val inputEntityPrevIntegrated=storage.readFromParquet[ContactPerson](config.inputPrevious).toDF()
    val prevRexIntegrated=storage.readFromParquet[ContactPersonRexLite](config.prevIntegrated).toDF()
    val input_entity_golden=storage.readFromParquet[ContactPersonGolden](config.inputUrl.replace(".parquet","_golden.parquet")).toDF()
    val input_delta=(input_entity.join(inputEntityPrevIntegrated,Seq("concatId"),JoinType.LeftAnti))

    val daily_merged_records:Dataset[ContactPersonRexLite]=transform(spark,input_delta,input_entity_golden)
    val dailyRefreshRexData=daily_merged_records
      .filter(!col("rexLiteMergedDate").contains("1970-01-01"))
      .select(prevRexIntegrated.columns.head, prevRexIntegrated.columns.tail: _*)
      .drop("additionalFields","ingestionErrors")
    val prevIntegRex=prevRexIntegrated.drop("additionalFields","ingestionErrors")

    val finalResult=(prevIntegRex.unionByName(dailyRefreshRexData))
      .drop("creationTimestamp")
      .withColumn("creationTimestamp", current_timestamp())
      .withColumn("additionalFields", typedLit(Map[String, String]()))
      .withColumn("ingestionErrors", typedLit(Map[String, IngestionError]()))
      .as[ContactPersonRexLite]

    storage.writeToParquet(finalResult, config.outputFile)
  }

}
