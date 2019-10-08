package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.OperatorGolden
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.DefaultConfig
import org.apache.spark.sql.SparkSession


object OperatorCreatePerfectGoldenRecord extends BaseMerging[OperatorGolden] {

  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {

    log.info(s"Creating golden operators records based on [${config.inputFile}] and writing them to [${config.outputFile}]")

    val entities = storage.readFromParquet[OperatorGolden](config.inputFile)

    val transformed = transform(spark, entities)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
