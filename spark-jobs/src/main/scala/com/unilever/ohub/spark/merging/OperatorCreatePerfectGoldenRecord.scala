package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.OperatorGolden
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{Constants, DefaultConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object OperatorCreatePerfectGoldenRecord extends BaseMerging[OperatorGolden] {
  // scalastyle:off
  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {
    import spark.implicits._
    val entity = storage.readFromParquet[OperatorGolden](config.inputFile)
    val entityAllCountries = entity
      .filter(!col("countryCode").isin("CH", "AT", "DE", "GB", "IE"))
    val entityDachCountries = entity
      .filter(col("countryCode").isin("CH", "AT", "DE"))
    val entityUkiCountries = entity
      .filter(col("countryCode").isin("GB", "IE"))

    val sourceListDachDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Constants.sourceListDach),
      StructType(Constants.schemaSourcePriority)
    )
    val sourceListUkiDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Constants.sourceListUki),
      StructType(Constants.schemaSourcePriority)
    )
    val groupPriorityWindow = Window.partitionBy("ohubId","priority")
    val orderByDateWindow = groupPriorityWindow.orderBy(
      when(col("dateUpdated").isNull, col("dateCreated"))
        .otherwise(col("dateUpdated")).desc_nulls_last,
      col("dateCreated").desc_nulls_last,
      col("ohubUpdated").desc
    )
    val groupOhubIdWindow = Window.partitionBy("ohubId")
    val orderBySourceWindow = groupOhubIdWindow.orderBy(
      col("priority").asc_nulls_last
    )

    val nonPreferredCol=Constants.nonPreferredColumns
    val dach_df=entityDachCountries.toDF()
      .join(sourceListDachDf,Seq("sourceName"),"left")
    val uki_df=entityUkiCountries.toDF()
      .join(sourceListUkiDf,Seq("sourceName"),"left")
    val outputDach=transform(spark,dach_df,orderByDateWindow,nonPreferredCol)
    val outputUki=transform(spark,uki_df,orderByDateWindow,nonPreferredCol)
    val outputGoldenDach=transform(spark,outputDach,orderBySourceWindow,nonPreferredCol)
      .drop("priority")
      .as[OperatorGolden]
    val outputGoldenUki=transform(spark,outputUki,orderBySourceWindow,nonPreferredCol)
      .drop("priority")
      .as[OperatorGolden]
    val outputGoldenOthers=transform(spark,entityAllCountries)
    val dachUkiGolden=outputGoldenDach.unionByName(outputGoldenUki)
    val allDataGolden=dachUkiGolden.unionByName(outputGoldenOthers)

    storage.writeToParquet(allDataGolden, config.outputFile)
  }
}
