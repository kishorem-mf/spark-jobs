package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.OperatorGolden
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.DefaultConfig
import org.apache.spark.sql.SparkSession


object OperatorCreatePerfectGoldenRecord extends BaseMerging[OperatorGolden] {

  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {
    val entity = storage.readFromParquet[OperatorGolden](config.inputFile)

    //val transformed = transform(spark, entity)

    storage.writeToParquet(entity, config.outputFile)
  }
}
