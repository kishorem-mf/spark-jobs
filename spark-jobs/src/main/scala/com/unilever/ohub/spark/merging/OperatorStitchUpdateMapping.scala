package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{DataFrame,Dataset, SparkSession}
import scopt.OptionParser

case class OperatorStitchUpdateMappingConfig(
                                               udlLookupMappingsInputFile: String = "udl-lookup-mappings-input-file",
                                               operatorsInputFile: String = "operators-input-prev-integ-file",
                                               outputFile: String = "path-to-output-file"
                                             ) extends SparkJobConfig

object OperatorStitchUpdateMapping extends SparkJob[OperatorStitchUpdateMappingConfig] {

  override def defaultConfig: OperatorStitchUpdateMappingConfig = OperatorStitchUpdateMappingConfig()

  def transform(
                 spark: SparkSession,
                 operators: DataFrame,
                 udlReferences: DataFrame
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
    cleaned_result
  }

  override private[spark] def configParser(): OptionParser[OperatorStitchUpdateMappingConfig] =
    new scopt.OptionParser[OperatorStitchUpdateMappingConfig]("Operators stitch column populate") {
      head("enriches operators with stitch id data", "1.0")
      opt[String]("udlLookupMappingsInputFile") required() action { (x, c) ⇒
        c.copy(udlLookupMappingsInputFile = x)
      } text "udlLookupMappingsInputFile is a string property"
      opt[String]("operatorsInputFile") required() action { (x, c) ⇒
        c.copy(operatorsInputFile = x)
      } text "operatorsInputFile is a string property"
      opt[String]("outputFile") optional() action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: OperatorStitchUpdateMappingConfig, storage: Storage): Unit = {
    val operators = storage.readFromParquet[Operator](config.operatorsInputFile)
    val udl_mappings = storage.readFromCsv(config.udlLookupMappingsInputFile,";",true)
    val transformed = transform(spark, operators.toDF(),udl_mappings)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
