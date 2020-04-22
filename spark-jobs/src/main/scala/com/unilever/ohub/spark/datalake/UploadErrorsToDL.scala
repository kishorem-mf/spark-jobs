package com.unilever.ohub.spark.datalake

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser
import java.sql.Timestamp

case class UploadErrosToDlConfig(
                                override val country: String = " country code",
                                override val previousIntegratedPath : String = "previous integrated file path",
                                override val outputPath : String = "output file path",
                                override val databaseUrl: String = "databaseUrl",
                                override val databaseUserName:String = "databaseUserName",
                                override val databasePassword:String = "databasePassword",
                                override val fromDate: String = "errors to fetch from date",
                                override val toDate: Option[String] = None
                                ) extends DataLakeConfig
//This case class is used to pass to the readFromParquet method that requires schemas for errors
case class Errors (
                  file_name: String,
                  line_number: Int,
                  error_message: String,
                  timestamp: Timestamp,
                  line: String,
                  rejected_value: String,
                  is_warning: Boolean,
                  country_code: String
                  ) extends Product

object UploadErrorsToDL extends SparkJob[UploadErrosToDlConfig] {

  override private[spark] def defaultConfig = UploadErrosToDlConfig()

  override private[spark] def configParser(): OptionParser[UploadErrosToDlConfig] =
    new scopt.OptionParser[UploadErrosToDlConfig]("get File Upload Error Insights") {
      head("Upload errors to DL ", "1.0")
      opt[String]("country_code") required() action { (x, c) ⇒
        c.copy(country = x)
      } text ("country to filter")
      opt[String]("previousIntegratedPath") required() action { (x, c) ⇒
        c.copy(previousIntegratedPath = x)
      } text ("Previous insight blob file path")
      opt[String]("outputPath") required() action { (x, c) ⇒
        c.copy(outputPath = x)
      } text ("Output blob path")
      opt[String]("databaseUrl") required() action { (x, c) ⇒
        c.copy(databaseUrl = x)
      } text ("URL of database")
      opt[String]("databaseUserName") required() action { (x, c) ⇒
        c.copy(databaseUserName = x)
      } text ("Username of database")
      opt[String]("databasePassword") required() action { (x, c) ⇒
        c.copy(databasePassword = x)
      } text ("Password of Database")
      opt[String]("fromDate") required() action { (x, c) ⇒
        c.copy(fromDate = x)
      } text ("from date to filter")
      opt[String]("toDate") optional() action { (x, c) ⇒
        c.copy(toDate = Some(x))
      } text ("To date to filter")

      version("1.0")
      help("help") text "help text"
    }

    override def run(spark: SparkSession, config: UploadErrosToDlConfig, storage: Storage): Unit = {
    implicit val sparkSession:SparkSession = spark
    val previousIntegratedErrorDF = storage.readFromParquet[Errors](config.previousIntegratedPath)
    val deltaIntegratedDF = DatalakeUtils.getCountryBasedUploadErrors(storage, config)
    val integratedErrorDF = previousIntegratedErrorDF.toDF("file_name",
      "line_number",
      "error_message",
      "timestamp",
      "line",
      "rejected_value",
      "is_warning",
      "country_code").unionByName(deltaIntegratedDF)//converting toDF since converting dataset to df
    storage.writeToParquet(integratedErrorDF,config.outputPath)
  }
}
