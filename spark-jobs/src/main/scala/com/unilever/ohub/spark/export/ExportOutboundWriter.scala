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
  val ACM, DISPATCHER = Value
}

case class OutboundConfig(
                           integratedInputFile: String = "integrated-input-file",
                           hashesInputFile: Option[String] = None,
                           targetType: TargetType = ACM,
                           outboundLocation: String = "outbound-location"
                         ) extends SparkJobConfig

abstract class ExportOutboundWriter[DomainType <: DomainEntity : TypeTag, OutboundType <: OutboundEntity] extends SparkJob[OutboundConfig] with CsvOptions {

  override private[spark] def defaultConfig = OutboundConfig()

  private[export] def goldenRecordOnlyFilter(spark: SparkSession, dataSet: Dataset[DomainType]) = dataSet.filter(_.isGoldenRecord)

  private[export] def filterDataSet(spark: SparkSession, dataSet: Dataset[DomainType]) = dataSet

  private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[DomainType]): Dataset[OutboundType]

  def entityName(): String

  val csvOptions = Map()

  def filename(targetType: TargetType): String = {
    val timestampFile = LocalDateTime.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    targetType match {
      case ACM ⇒ "UFS_" + entityName() + "_" + timestampFile + ".csv"
      case DISPATCHER ⇒ "UFS_DISPATCHER" + "_" + entityName() + "_" + timestampFile + ".csv"
    }
  }

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

    val columnsInOrder = domainEntities.columns :+ "hasChanged"
    val result: Dataset[DomainType] = filterDataSet(spark, domainEntities)
      .join(hashesInputFile, Seq("concatId"), JoinType.LeftOuter)
      .withColumn("hasChanged", when('hasChanged.isNull, true).otherwise('hasChanged))
      .filter($"hasChanged")
      .select(columnsInOrder.head, columnsInOrder.tail: _*)
      .as[DomainType]

    writeToCsv(config, convertDataSet(spark, result), spark)
  }

  def writeToCsv(config: OutboundConfig, ds: Dataset[OutboundType], sparkSession: SparkSession): Unit = {
    val outputFilePath = new Path(config.outboundLocation)
    val temporaryPath = new Path(outputFilePath, UUID.randomUUID().toString)
    ds
      .write
      .mode(SaveMode.Overwrite)
      .option("encoding", "UTF-8")
      .option("header", "false")
      .options(options)
      .csv(temporaryPath.toString)

    val header = ds.columns.map(c ⇒ if (mustQuotesFields) "\"" + c + "\"" else c).mkString(delimiter)
    mergeDirectoryToOneFile(temporaryPath, new Path(outputFilePath, filename(config.targetType)), sparkSession, header)
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
