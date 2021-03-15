package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.OperatorUfsGolden
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.DefaultConfig
import org.apache.spark.sql.SparkSession


object OperatorCreatePerfectUfsGoldenRecord extends BaseMerging[OperatorUfsGolden] {

  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {
    val entity = storage.readFromParquet[OperatorUfsGolden](config.inputFile)

    val transformed = transform(spark, entity)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
