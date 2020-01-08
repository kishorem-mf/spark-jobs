package com.unilever.ohub.spark.datalake

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{DomainDataProvider, SparkJob, SparkJobConfig}
import scopt.OptionParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession

case class CopyToDLConfig(
                           incomingRawPath: String = "incomingRawPath",
                           datalakeRawPath: String = "datalakeRawPath",
                           countries: String = "countries",
                           folderDate: String = "folderDate"
                         ) extends SparkJobConfig

object FilterWithCountryToDL extends SparkJob[CopyToDLConfig] {

  override private[spark] def configParser(): OptionParser[CopyToDLConfig] =
    new scopt.OptionParser[CopyToDLConfig]("Copy raw data to datalake") {
      head("change log entity output file.", "1.0")
      opt[String]("incomingRawPath") required() action { (x, c) ⇒
        c.copy(incomingRawPath = x)
      } text "changeLogIntegrated is a string property"
      opt[String]("datalakeRawPath") required() action { (x, c) ⇒
        c.copy(datalakeRawPath = x)
      } text "changeLogPrevious is a string property"
      opt[String]("countries") required() action { (x, c) ⇒
        c.copy(countries = x)
      } text "countries to copy"
      opt[String]("folderDate") required() action { (x, c) ⇒
        c.copy(folderDate = x)
      } text "date for datalake folder"

      version("1.0")
      help("help") text "help text"
    }

  override private[spark] def defaultConfig = CopyToDLConfig()


  override def run(spark: SparkSession, config: CopyToDLConfig, storage: Storage): Unit = {

    import spark.implicits._
    def listCsvFiles(basep: String): Seq[String] = {
      val conf = new Configuration(spark.sparkContext.hadoopConfiguration)
      val fs = FileSystem.get(new URI(basep), conf)
      storage.getCsvFilePaths(fs, new Path(basep)).map(_.toString)
    }

    def transform(incomingRawPath: String, datalakeRawPath: String, salesOrgToCountryMap: Map[String, String]) = {
      listCsvFiles(incomingRawPath).foreach { file =>
        val df = storage.readFromCsv(file, ";")
        config.countries.split(",").foreach { country =>
          val cols = df.columns.toSeq
          val dropCols = cols.filter(_.startsWith("_"))
          val filterDF = (cols.map(_.toUpperCase) match {
            case col if col.contains("COUNTRY_CODE") => df.filter($"Country_Code" === country)
            case col if col.contains("COUNTRY CODE") => df.filter($"Country Code" === country)
            case _ => df.columns.find(_ == "SalesOrg") match {
              case Some(column) => df.filter(col(column) === salesOrgToCountryMap(country))
              case None => df
            }
          }).drop(dropCols: _*)
          val fileName = file.split("/").last
          println("::::::::::::::::::::::::::::::::::::::::::::::::::::::") //scalastyle:ignore
          println(s"$datalakeRawPath$country/${config.folderDate}") //scalastyle:ignore
          DatalakeUtils.writeToCsv(s"$datalakeRawPath$country/${config.folderDate}", fileName, filterDF, spark)
        }
      }
    }

    transform(config.incomingRawPath, config.datalakeRawPath, DomainDataProvider().salesOrgToCountryMap)

  }

}






