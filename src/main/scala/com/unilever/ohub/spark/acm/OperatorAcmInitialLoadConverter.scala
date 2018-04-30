package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.{ DefaultWithDbConfig, SparkJobWithDefaultDbConfig }
import com.unilever.ohub.spark.acm.model.UFSOperator
import com.unilever.ohub.spark.data.ChannelMapping
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainDataProvider
import org.apache.spark.sql.{ Dataset, SparkSession }

object OperatorAcmInitialLoadConverter extends SparkJobWithDefaultDbConfig {

  def transform(
    spark: SparkSession,
    channelMappings: Dataset[ChannelMapping],
    operators: Dataset[Operator]
  ): Dataset[UFSOperator] = {
    OperatorAcmConverter.createUfsOperators(spark, operators, channelMappings)
  }

  override def run(spark: SparkSession, config: DefaultWithDbConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating operator ACM csv file from [$config.inputFile] to [$config.outputFile]")

    val dataProvider = DomainDataProvider(spark, config.postgressUrl, config.postgressDB, config.postgressUsername, config.postgressPassword)

    val channelMappings = dataProvider.channelMappings()
    val operators = storage.readFromParquet[Operator](config.inputFile)
    val transformed = transform(spark, channelMappings, operators)

    OperatorAcmConverter.writeToCsv(storage, transformed, config.outputFile)
  }
}
