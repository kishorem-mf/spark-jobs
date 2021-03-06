package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{Constants, SparkJob, SparkJobConfig}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import scopt.OptionParser
import java.sql.Date

import com.unilever.ohub.spark.sql.JoinType

case class Record(dateUpdated:  Date, concat_source: String, concat_caterlyst: String) extends scala.Product

case class OperatorStitchUpdateMappingConfig(
                                              udlLookupMappingsInputFile: String = "udl-lookup-mappings-input-file",
                                              operatorsInputFile: String = "operators-input-prev-integ-file",
                                              outputFile: String = "path-to-output-file",
                                              deltaPreProcessedOutputFile: String = "operators-preprocessed-file"
                                            ) extends SparkJobConfig

object OperatorStitchUpdateMapping extends SparkJob[OperatorStitchUpdateMappingConfig] {

  override def defaultConfig: OperatorStitchUpdateMappingConfig = OperatorStitchUpdateMappingConfig()

  def transform(
                 spark: SparkSession,
                 operators: DataFrame,
                 udlReferences: DataFrame,
                 deltaPreProcessedOutput: Dataset[Operator]
               ): Dataset[Operator] = {
    import spark.implicits._
    val operator_udl=operators.join(udlReferences.select("concat_source","concat_caterlyst"),
      operators("concatId")===udlReferences("concat_source"))
    val rename_columns=operators.select($"ohubId".as("prev_ohubId"),$"concatId".as("prev_concatId"))
    val uncleaned_output=rename_columns.join(operator_udl,$"concat_caterlyst"===$"prev_concatId")
    val cleaned_result=uncleaned_output.drop("oldIntegrationId","concat_source","concat_caterlyst")
      .withColumn("oldIntegrationId",uncleaned_output("prev_ohubId"))
      .drop("prev_ohubId","prev_concatId")
      .as[Operator]
    val combine=cleaned_result.unionByName(deltaPreProcessedOutput)
    combine
  }

  override private[spark] def configParser(): OptionParser[OperatorStitchUpdateMappingConfig] =
    new scopt.OptionParser[OperatorStitchUpdateMappingConfig]("Operators stitch column populate") {
      head("enriches operators with stitch id data", "1.0")
      opt[String]("udlLookupMappingsInputFile") required() action { (x, c) ???
        c.copy(udlLookupMappingsInputFile = x)
      } text "udlLookupMappingsInputFile is a string property"
      opt[String]("operatorsInputFile") required() action { (x, c) ???
        c.copy(operatorsInputFile = x)
      } text "operatorsInputFile is a string property"
      opt[String]("outputFile") optional() action { (x, c) ???
        c.copy(outputFile = x)
      } text "outputFile is a string property"
      opt[String]("deltaPreProcessedOutputFile") optional() action { (x, c) ???
        c.copy(deltaPreProcessedOutputFile = x)
      } text "deltaPreProcessedOutputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: OperatorStitchUpdateMappingConfig, storage: Storage): Unit = {
    import spark.implicits._
    val operators = storage.readFromParquet[Operator](config.operatorsInputFile)
    val operator_preprocessed=storage.readFromParquet[Operator](config.deltaPreProcessedOutputFile)
    val textRDD = spark.sparkContext.textFile(config.udlLookupMappingsInputFile)
    val format=new java.text.SimpleDateFormat("yyyy-MM-dd")
    val records = textRDD.map {
      line =>
        val col = line.split(";")
        Record(new java.sql.Date(format.parse(col(0)).getTime()), col(1), col(2))
    }

    val result = records.groupBy(_.concat_source).map { _._2.reduce {
      (r1: Record, r2: Record) => if (r1.dateUpdated.after(r2.dateUpdated)) r1 else r2
    }}
    val dfNoDuplicateSources = spark.createDataFrame(result)
    val transformed = transform(spark, operators.toDF(),dfNoDuplicateSources,operator_preprocessed)
    storage.writeToParquet(transformed, config.outputFile)

    val prev_operator = operators.join(transformed,Seq("concatId"),JoinType.LeftAnti).as[Operator]
    storage.writeToParquet(prev_operator, config.operatorsInputFile)
  }
}
