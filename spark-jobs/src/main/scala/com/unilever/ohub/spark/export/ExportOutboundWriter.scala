package com.unilever.ohub.spark.export

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.unilever.ohub.spark.domain.DomainEntity
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
                           previousIntegratedInputFile: Option[String] = None,
                           targetType: TargetType = ACM,
                           outboundLocation: String = "outbound-location",
                           countryCodes: Option[Seq[String]] = None,
                           mappingOutputLocation: Option[String] = None
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
      opt[String]("previousIntegratedInputFile") optional() action { (x, c) ⇒
        c.copy(previousIntegratedInputFile = Some(x))
      } text "previousIntegratedInputFile is a string property"
      opt[Seq[String]]("countryCodes") optional() action { (x, c) =>
        c.copy(countryCodes = Some(x))
      } text "countryCodes is a string array"
      opt[String]("mappingOutputLocation") optional() action { (x, c) =>
        c.copy(mappingOutputLocation = Some(x))
      } text "mappingOutputFile is a string property"
      version("1.0")
      help("help") text "help text"
    }

  override private[spark] def defaultConfig = OutboundConfig()
}

object ExportOutboundWriter {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def jsonStringToNode(jsonString: String, isRetry: Boolean = false): JsonNode = {
    if (jsonString != "") mapper.readTree(jsonString) else mapper.createObjectNode()
  }
}

abstract class ExportOutboundWriter[DomainType <: DomainEntity : TypeTag] extends SparkJobWithOutboundExportConfig with CsvOptions {

  override private[spark] def defaultConfig = OutboundConfig()

  private[export] def goldenRecordOnlyFilter(spark: SparkSession, dataSet: Dataset[DomainType]) = dataSet.filter((row: DomainType) => row.isGoldenRecord)

  private[export] def filterDataSet(spark: SparkSession, dataSet: Dataset[DomainType], config: OutboundConfig) = dataSet

  private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[DomainType]): Dataset[_]

  private[export] def explainConversion: Option[DomainType => _ <: OutboundEntity] = None

  private[export] def getDeletedOhubIdsFromPreviousIntegrated(spark: SparkSession, filteredChanges: Dataset[DomainType], previousIntegratedEntities: Dataset[DomainType], integratedEntities: Dataset[DomainType]) = filteredChanges


  def entityName(): String

  val csvOptions = Map()

  val onlyExportChangedRows = true

  def mergeCsvFiles(targetType: TargetType):Boolean = true
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

    val previousIntegratedFile = config.previousIntegratedInputFile match {
      case Some(location) => storage.readFromParquet[DomainType](location)
      case None => spark.createDataset[DomainType](Seq())
    }

    export(storage.readFromParquet[DomainType](config.integratedInputFile), previousIntegratedFile, config, spark);
  }

  def export(integratedEntities: Dataset[DomainType], previousIntegratedEntities: Dataset[DomainType], config: OutboundConfig, spark: SparkSession) {
    import spark.implicits._

    val domainEntities = config.targetType match {
      case ACM ⇒ goldenRecordOnlyFilter(spark, integratedEntities)
      case _ ⇒ integratedEntities
    }

    val filteredByCountries = if (config.countryCodes.isDefined) domainEntities.filter($"countryCode".isin(config.countryCodes.get: _*)) else domainEntities

    val columnsInOrder = filteredByCountries.columns
    val filtered: Dataset[DomainType] = filterDataSet(spark, filteredByCountries, config)

    val filteredEntities = if (onlyExportChangedRows) filterOnlyChangedRows(filtered, previousIntegratedEntities, spark) else filtered

    val processedChanged = getDeletedOhubIdsFromPreviousIntegrated(spark, filteredEntities, previousIntegratedEntities, integratedEntities)

    val result = processedChanged
      .select(columnsInOrder.head, columnsInOrder.tail: _*)
      .as[DomainType]

    if (config.mappingOutputLocation.isDefined && explainConversion.isDefined && domainEntities.head(1).nonEmpty) {
      log.info(s"ConversionExplanation found, writing output to ${config.mappingOutputLocation.get}")
      val mapping = explainConversion.get.apply(domainEntities.head)
      writeToJson(spark, new Path(config.mappingOutputLocation.get), deserializeJsonFields(mapping))
    }

    writeToCsv(config, convertDataSet(spark, result), spark)
  }

  def filterOnlyChangedRows(dataset: Dataset[DomainType], previousIntegratedFile: Dataset[DomainType], spark: SparkSession): Dataset[DomainType] = {
    import spark.implicits._

    val excludedColumns = Seq(
      "id",
      "creationTimestamp",
      "ohubCreated",
      "ohubUpdated",
      "additionalFields",
      "ingestionErrors"
    )

    val originalJoinColumns = dataset.columns.filter(!excludedColumns.contains(_))

    // This will result in forming condition "dataset(col1) <=> prevInteg(col1) AND dataset(col2) <=> prevInteg(col2)  and so on
    val joinClause = originalJoinColumns
                     .map((columnName: String) => dataset(columnName) <=> previousIntegratedFile(columnName))
                     .reduce((prev, curr) => prev && curr)

    dataset.join(previousIntegratedFile, joinClause, JoinType.LeftAnti).as[DomainType]

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

    if (mergeCsvFiles(config.targetType)) {
      writeableData.csv(temporaryPath.toString)
      val header = ds.columns.map(c ⇒ if (mustQuotesFields) "\"" + c + "\"" else c).mkString(delimiter)
      mergeDirectoryToOneFile(temporaryPath, outputFilePath, sparkSession, header)
    } else {
      writeableData.csv(outputFilePath.toString)
    }
  }

  def mergeDirectoryToOneFile(sourceDirectory: Path, outputFile: Path, spark: SparkSession, header: String): Boolean = {
    log.info(s"Merging to one directory [${sourceDirectory}] to ${outputFile}")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    //Create a new header file which start with a `_` because the files are merged based on alphabetical order so
    //the header files will be merged first
    createHeaderFile(fs, sourceDirectory, header)

    def moveOnlyCsvFilesToOtherDirectory = {
      //Move all csv files to different directory so we don't make a mistake of merging other files from the source directory
      val tmpCsvSourceDirectory = new Path(sourceDirectory.getParent, UUID.randomUUID().toString)
      fs.mkdirs(tmpCsvSourceDirectory)
      fs.listStatus(sourceDirectory)
        .filter(p ⇒ p.isFile)
        .filter(p ⇒ p.getPath.getName.endsWith(".csv"))
        .map(_.getPath)
        .foreach(fs.rename(_, tmpCsvSourceDirectory))
      tmpCsvSourceDirectory
    }

    val tmpCsvSourceDirectory: Path = moveOnlyCsvFilesToOtherDirectory
    FileUtil.copyMerge(fs, tmpCsvSourceDirectory, fs, outputFile, true, spark.sparkContext.hadoopConfiguration, null) // scalastyle:ignore
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

  /**
   * This function works for objects that only contain string fields with JSON content. The object is transformed to
   * a map with jsonNodes so the mapper can convert it to one JSON object (without escaped JSON as values).
   *
   * F.e. {"key": "{\"name\": \"nested object\"}"}
   * will become a map[String, JsonNode] which can be written (by the objectMapper) like
   * {"key": {"name": "nested object"}}
   *
   * @param subject
   * @return
   */
  private def deserializeJsonFields(subject: Any): Map[String, Any] = {
    (Map[String, Any]() /: subject.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> ExportOutboundWriter.jsonStringToNode(f.get(subject).asInstanceOf[String]))
    }
  }

  private def writeToJson(spark: SparkSession, outputDirectory: Path, subject: Any): Unit = {
    import java.io.{BufferedWriter, OutputStreamWriter}
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val out = fs.create(outputDirectory, true)
    val br = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"))
    try {
      ExportOutboundWriter.mapper.writeValue(br, subject)
    } finally {
      if (out != null) br.close()
    }
  }

  private[export] def getDeletedOhubIdsWithTargetId(spark: SparkSession, prevIntegratedDS: Dataset[DomainType], integratedDS: Dataset[DomainType]) = {

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val appendTargetOhubIdToAdditionalFields = (additionalFieldsMap: Map[String, String], targetOhubId: String) =>
      additionalFieldsMap + ("targetOhubId" -> targetOhubId)
    val setAdditionalFields = udf(appendTargetOhubIdToAdditionalFields)

    val deletedOhubIdDataset = prevIntegratedDS
      .filter($"isGoldenRecord")
      .join(integratedDS.filter($"isGoldenRecord"), Seq("ohubId"), "left_anti")
      .withColumn("isActive", lit(false))

    deletedOhubIdDataset
      .join(integratedDS, Seq("concatId"), "left")
      .select(deletedOhubIdDataset("*"), integratedDS("ohubId") as ("targetOhubId"))
      .withColumn("additionalFields", setAdditionalFields($"additionalFields", $"targetOhubId"))
      .drop("targetOhubId")
      .as[DomainType]

  }
}
