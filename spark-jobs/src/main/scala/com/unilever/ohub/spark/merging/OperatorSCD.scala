package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.SparkSession

object OperatorSCD extends BaseSCD[Operator] {

  override def run(spark: SparkSession, config: ChangeLogConfig, storage: Storage): Unit = {
    log.info(s"Creating an SCD2 of ohubid and concatid for the operator entity")

    val entities = storage.readFromParquet[Operator](config.changeLogIntegrated)

    val changeLogPrevious = spark.read.parquet(config.changeLogPrevious)

    val transformed = transform(spark, entities, changeLogPrevious)

    storage.writeToParquet(transformed, config.changeLogOutput)

  }
}


