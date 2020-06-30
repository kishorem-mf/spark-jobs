package com.unilever.ohub.spark.datalake

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{DomainDataProvider, SparkJob, SparkJobConfig}
import scopt.OptionParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

import com.unilever.ohub.spark.datalake.DatalakeUtils.getFolderDateList
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.util.Try

case class CopyToDLConfig(
                          incomingRawPath: String = "incomingRawPath",
                          datalakeRawPath: String = "datalakeRawPath",
                          countries: String = "countries",
                          fromDate: String = "fromDate",
                          toDate: Option[String] = None
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
      opt[String]("fromDate") required() action { (x, c) ⇒
        c.copy(fromDate = x)
      } text "date for datalake folder"
      opt[String]("toDate") optional() action { (x, c) ⇒
        c.copy(toDate = Some(x))
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
      Try(fs.listStatus(new Path(basep)).isEmpty).toOption match {
        case Some(_) => storage.getCsvFilePath(fs, new Path(basep)).map(_.toString)
        case None => None.toSeq}}

    def countryCodeSelection(salesOrgToCountryMap: Map[String, String], df: Dataset[Row], country: String, cols: Seq[String], dropCols: Seq[String]) = {
      val filterDF = (cols.map(_.toUpperCase) match {
        case col if col.contains("COUNTRY_CODE") => df.filter($"Country_Code" === country)
        case col if col.contains("COUNTRY CODE") => df.filter($"Country Code" === country)
        case col if col.contains("COUNTRYCODE") => df.filter($"countryCode" === country)
        case _ => df.columns.find(_ == "SalesOrg") match {
          case Some(column) => df.filter(col(column) === salesOrgToCountryMap.getOrElse(country, "ANY_STRING"))
          case None => df
        }
      }).drop(dropCols: _*)
      filterDF}

    def transform(incomingRawPath: String, datalakeRawPath: String, folderDate: String, salesOrgToCountryMap: Map[String, String]) = {
       listCsvFiles(incomingRawPath).foreach { file =>
        val df = storage.readFromCsv(file, ";")
        val scFile = spark.sparkContext.textFile(file)
        config.countries.split(",").map(_.trim).foreach { country =>
          val cols = df.columns.toSeq
          val dropCols = cols.filter(_.startsWith("_"))
          val filterDF: DataFrame = countryCodeSelection(salesOrgToCountryMap, df, country, cols, dropCols)
          Try(filterDF.first).toOption match {
            case Some(_) => val fileName = file.split("/").last
              DatalakeUtils.writeToCsv(s"$datalakeRawPath$country/${folderDate}", fileName, filterDF, scFile, spark)
            case None => log.debug(s"No record for $country in ${file}")
          }
        }
      }
    }
    val folderDates = getFolderDateList(config.fromDate, config.toDate.getOrElse(config.fromDate).toString)
    folderDates.foreach{ folderDate=>
      val blobFolderPath = config.incomingRawPath + folderDate + "/"
      transform(blobFolderPath, config.datalakeRawPath, folderDate, DomainDataProvider().salesOrgToCountryMap)}
  }
}






