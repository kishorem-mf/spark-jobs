package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.DefaultConfig
import org.apache.spark.sql.SparkSession

object ContactPersonCreateGoldenRecord extends BaseMerging[ContactPerson] {

  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {
    log.info(s"Creating golden contact persons records based on [${config.inputFile}] and writing them to [${config.outputFile}]")

    val entity = storage.readFromParquet[ContactPerson](config.inputFile)

    val transformed = transform(spark, entity)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
