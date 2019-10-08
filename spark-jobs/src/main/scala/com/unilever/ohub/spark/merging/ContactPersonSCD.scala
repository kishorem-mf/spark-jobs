package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.{ContactPerson, Operator}
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.SparkSession

object ContactPersonSCD extends BaseSCD[ContactPerson] {

  override def run(spark: SparkSession, config: ChangeLogConfig, storage: Storage): Unit = {
    log.info(s"Creating an SCD2 of ohubid and concatid for the contact person entity")

    val entities = storage.readFromParquet[ContactPerson](config.changeLogIntegrated)

    val changeLogPrevious = spark.read.parquet(config.changeLogPrevious)

    val transformed = transform(spark, entities, changeLogPrevious)

    storage.writeToParquet(transformed, config.changeLogOutput)
  }
}


