package com.unilever.ohub.spark.export

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityHash}
import com.unilever.ohub.spark.export.TargetType.{ACM, DISPATCHER, TargetType}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import scopt.OptionParser

import scala.reflect.runtime.universe._

object TargetType extends Enumeration {
  type TargetType = Value
  val ACM, DISPATCHER, DATASCIENCE, MEPS = Value
}

case class OutboundConfig(
                           integratedInputFile: String = "integrated-input-file",
                           hashesInputFile: Option[String] = None,
                           targetType: TargetType = ACM,
                           outboundLocation: String = "outbound-location"
                         ) extends SparkJobConfig

abstract class SparkJobWithOutboundExportConfig extends SparkJob[OutboundConfig] {
  override private[spark] def configParser(): OptionParser[OutboundConfig] =
    new scopt.OptionParser[OutboundConfig]("Spark job default") {
      head("run a spark job with default config.", "1.0")

      opt[String]("outbound-location") required() action { (x, c) ⇒
        c.copy(outboundLocation = x)
      } text "outbound-location is a string property"
      opt[String]("targetType") required() action { (x, c) ⇒
        c.copy(targetType = TargetType.withName(x))
      } text "targetType is a string property"
      opt[String]("integratedInputFile") required() action { (x, c) ⇒
        c.copy(integratedInputFile = x)
      } text "integratedInputFile is a string property"
      opt[String]("hashesInputFile") optional() action { (x, c) ⇒
        c.copy(hashesInputFile = Some(x))
      } text "hashesInputFile is a string property"
      version("1.0")
      help("help") text "help text"
    }

  override private[spark] def defaultConfig = OutboundConfig()
}

abstract class ExportOutboundWriter[DomainType <: DomainEntity : TypeTag] extends SparkJobWithOutboundExportConfig with CsvOptions {

  override private[spark] def defaultConfig = OutboundConfig()

  private[export] def goldenRecordOnlyFilter(spark: SparkSession, dataSet: Dataset[DomainType]) = dataSet.filter(_.isGoldenRecord)

  private[export] def filterDataSet(spark: SparkSession, dataSet: Dataset[DomainType], config: OutboundConfig) = dataSet

  private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[DomainType]): Dataset[_]

  def entityName(): String

  val csvOptions = Map()

  val onlyExportChangedRows = true

  def mergeCsvFiles(targetType: TargetType) = true
  // When merging, headers are based on the dataset columns (and not writen by DataSet.write.csv)
  private def shouldWriteHeaders(targetType: TargetType) = (!mergeCsvFiles(targetType)).toString

  def filename(targetType: TargetType): String = {
    val timestampFile = LocalDateTime.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    targetType match {
      case ACM ⇒ "UFS_" + entityName() + "_" + timestampFile + ".csv"
      case DISPATCHER ⇒ "UFS_DISPATCHER" + "_" + entityName() + "_" + timestampFile + ".csv"
    }
  }

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"writing integrated entities [${config.integratedInputFile}] to outbound export csv file for ACM and DDB.")

    val hashesInputFile = config.hashesInputFile match {
      case Some(location) ⇒ storage.readFromParquet[DomainEntityHash](location)
      case None ⇒ spark.createDataset(Seq[DomainEntityHash]())
    }
    export(storage.readFromParquet[DomainType](config.integratedInputFile), hashesInputFile, config, spark);
  }

  def export(integratedEntities: Dataset[DomainType], hashesInputFile: Dataset[DomainEntityHash], config: OutboundConfig, spark: SparkSession) {
    import spark.implicits._

    val domainEntities = config.targetType match {
      case ACM ⇒ goldenRecordOnlyFilter(spark, integratedEntities)
      case _ ⇒ integratedEntities
    }

    val columnsInOrder = domainEntities.columns
    val filtered: Dataset[DomainType] = filterDataSet(spark, domainEntities, config)

    val processedChanged = if(onlyExportChangedRows) filterOnlyChangedRows(filtered, hashesInputFile, spark) else filtered

    val result = processedChanged
      .select(columnsInOrder.head, columnsInOrder.tail: _*)
      .as[DomainType]

    writeToCsv(config, convertDataSet(spark, result), spark)
  }

  def filterOnlyChangedRows(dataset: Dataset[DomainType], hashesInputFile: Dataset[DomainEntityHash], spark: SparkSession) : Dataset[DomainType] = {
    import spark.implicits._

    dataset
      .join(hashesInputFile, Seq("concatId"), JoinType.LeftOuter)
      .withColumn("hasChanged", when('hasChanged.isNull, true).otherwise('hasChanged))
      .filter($"hasChanged")
      .drop("hasChanged")
      .as[DomainType]
  }

  def writeToCsv(config: OutboundConfig, ds: Dataset[_], sparkSession: SparkSession): Unit = {
    val outputFolderPath = new Path(config.outboundLocation)
    val temporaryPath = new Path(outputFolderPath, UUID.randomUUID().toString)
    val outputFilePath = new Path(outputFolderPath, filename(config.targetType))
    val writeableData = ds
      .write
      .mode(SaveMode.Overwrite)
      .option("encoding", "UTF-8")
      .option("header", shouldWriteHeaders(config.targetType))
      .options(options)

    if(mergeCsvFiles(config.targetType)) {
      writeableData.csv(temporaryPath.toString)
      val header = ds.columns.map(c ⇒ if (mustQuotesFields) "\"" + c + "\"" else c).mkString(delimiter)
      mergeDirectoryToOneFile(temporaryPath, outputFilePath, sparkSession, header)
    } else writeableData.csv(outputFilePath.toString)
  }

  def mergeDirectoryToOneFile(sourceDirectory: Path, outputFile: Path, spark: SparkSession, header: String) = {
    log.info(s"Merging to one directory [${sourceDirectory}] to ${outputFile}")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    //Create a new header file which start with a `_` because the files are merged based on alphabetical order so
    //the header files will be merged first
    createHeaderFile(fs, sourceDirectory, header)

    def moveOnlyCsvFilesToOtherDirectory = {
      //Move all csv files to different directory so we don't make a mistake of merging other files from the source directory
      val tmpCsvSourceDirectory = new Path(sourceDirectory.getParent, UUID.randomUUID().toString)
      fs.mkdirs(tmpCsvSourceDirectory)
      val csvFiles = fs.listStatus(sourceDirectory)
        .filter(p ⇒ p.isFile)
        .filter(p ⇒ p.getPath.getName.endsWith(".csv"))
        .map(_.getPath)
        .map(fs.rename(_, tmpCsvSourceDirectory))
      tmpCsvSourceDirectory
    }

    val tmpCsvSourceDirectory: Path = moveOnlyCsvFilesToOtherDirectory
    FileUtil.copyMerge(fs, tmpCsvSourceDirectory, fs, outputFile, true, spark.sparkContext.hadoopConfiguration, null)
    fs.delete(sourceDirectory, true)
  }

  def createHeaderFile(fs: FileSystem, sourceDirectory: Path, header: String): Path = {
    import java.io.{BufferedWriter, OutputStreamWriter}

    val headerFile = new Path(sourceDirectory, "_" + UUID.randomUUID().toString + ".csv")
    val out = fs.create(headerFile)
    val br = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"))
    try {
      br.write(header + "\n")
    } finally {
      if (out != null) br.close()
    }
    headerFile
  }
}
