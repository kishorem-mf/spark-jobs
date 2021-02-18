package com.unilever.ohub.spark.ingest

import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import com.unilever.ohub.spark.datalake.DatalakeUtils
import org.apache.spark.sql.functions._
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

case class CombineDeptDataConfig(
                                  ufsDeptData: String = "ufs-input-file",
                                  oohDeptData: String = "ooh-integrated-file",
                                  outputFile: String = "path-to-output-file"
                                ) extends SparkJobConfig

object CombineDeptData extends SparkJob[CombineDeptDataConfig] {

  def transform(
                 spark: SparkSession,
                 ufsDeptData: DataFrame,
                 oohDeptData: DataFrame
               ): DataFrame = {
    import spark.implicits._
    // scalastyle:off
    // null values are added as they are empty cells
    def customSelect(availableCols: List[String], requiredCols: List[String]) = {
      requiredCols.map(column => column match {
        case column if availableCols.contains(column) => col(column)
        case _ => lit(null).as(column)
      })
    }
    // scalastyle:on
    val combinedData = ufsDeptData.select(customSelect(
      ufsDeptData.columns.toList,
      oohDeptData.columns.toList)
      :_*).unionByName(oohDeptData)
    combinedData
  }

  override private[spark] def defaultConfig = CombineDeptDataConfig()

  override private[spark] def configParser(): OptionParser[CombineDeptDataConfig] =
    new scopt.OptionParser[CombineDeptDataConfig]("Department merging") {
      head("merges department into an output file.", "1.0")
      opt[String]("ufsDeptData") required () action { (x, c) ⇒
        c.copy(ufsDeptData = x)
      } text "ufsDeptData is a string property"
      opt[String]("oohDeptData") required () action { (x, c) ⇒
        c.copy(oohDeptData = x)
      } text "oohDeptData is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: CombineDeptDataConfig, storage: Storage): Unit = {
    val ufsData = storage.readFromCsv(config.ufsDeptData,";",true,"\"")
    val oohData = storage.readFromCsv(config.oohDeptData,";",true,"\"")
    val transformed = transform(spark, ufsData, oohData)

    DatalakeUtils.writeToCsv(config.outputFile,"UFS_ALL_DATA_PROCESSED.csv", transformed,spark)
  }
}
