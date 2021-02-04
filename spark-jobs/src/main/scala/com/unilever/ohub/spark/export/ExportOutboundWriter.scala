package com.unilever.ohub.spark.export

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.export.TargetType.{ACM, DDL, DISPATCHER, TargetType}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import scopt.OptionParser

import scala.reflect.runtime.universe._

object TargetType extends Enumeration {
  type TargetType = Value
  val ACM, DISPATCHER, DATASCIENCE, MEPS, AURORA, DDL = Value
}

case class OutboundConfig(
                           integratedInputFile: String = "integrated-input-file",
                           previousIntegratedInputFile: Option[String] = None,
                           targetType: TargetType = ACM,
                           outboundLocation: String = "outbound-location",
                           countryCodes: Option[Seq[String]] = None,
                           mappingOutputLocation: Option[String] = None,
                           currentMerged: Option[String] = None,
                           previousMerged: Option[String] = None,
                           currentMergedOPR: Option[String] = None,
                           excludeCountryCodes: String = "Excluded countries",
                           auroraCountryCodes: String = "",
                           integratedDate: String = "",
                           fromDate: String = "",
                           toDate: String = ""
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
      opt[String]("currentMergedIntegratedInputFile") optional() action { (x, c) ⇒
        c.copy(currentMerged = Some(x))
      } text "current Merged Integrated InputFile is a string property"
      opt[String]("previousMergedIntegratedInputFile") optional() action { (x, c) ⇒
        c.copy(previousMerged = Some(x))
      } text "previous Merged Integrated is a string property"
      opt[String]("excludeCountryCodes") optional() action { (x, c) =>
        c.copy(excludeCountryCodes = x)
      } text "exclude countryCodes is a string array"
      opt[String]("auroraCountryCodes") optional() action { (x, c) =>
        c.copy(auroraCountryCodes = x)
      } text "aurora countryCodes is a string array"
      opt[String]("integratedDate") optional() action { (x, c) =>
        c.copy(integratedDate = x)
      } text "integratedDate is a string array"
      opt[String]("fromDate") optional() action { (x, c) ⇒
        c.copy(fromDate = x)
      } text "from date is a string property"
      opt[String]("toDate") optional() action { (x, c) ⇒
        c.copy(toDate = x)
      } text "to date is a string property"
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

  private[export] def goldenRecordOnlyFilter(spark: SparkSession, dataSet: Dataset[DomainType]) = dataSet.filter((row: DomainType) => (row.isGoldenRecord))

  private[export] def entitySpecificFilter(spark: SparkSession, dataSet: Dataset[DomainType], config: OutboundConfig) = dataSet

  private[export] def filterValid[GenericOutboundEntity <: OutboundEntity](spark: SparkSession, dataSet: Dataset[_], config: OutboundConfig) = dataSet

  private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[DomainType]): Dataset[_]

  private[export] def explainConversion: Option[DomainType => _ <: OutboundEntity] = None

  private[export] def linkOperator[GenericOutboundEntity <: OutboundEntity](spark: SparkSession, operatorDS: Dataset[_], deltaDs: Dataset[_]): Dataset[_] = deltaDs

  def entityName(): String

  val csvOptions = Map()

  val onlyExportChangedRows = true

  def mergeCsvFiles(targetType: TargetType): Boolean = true

  // When merging, headers are based on the dataset columns (and not writen by DataSet.write.csv)
  private def shouldWriteHeaders(targetType: TargetType) = (!mergeCsvFiles(targetType)).toString

  def filename(targetType: TargetType): String = {
    val timestampFile = LocalDateTime.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    targetType match {
      case ACM ⇒ "UFS_" + entityName() + "_" + timestampFile + ".csv"
      case DISPATCHER ⇒ "UFS_DISPATCHER" + "_" + entityName() + "_" + timestampFile + ".csv"
      case DDL ⇒ "UFS_DDL_" + entityName() + "_" + timestampFile + "_" + UUID.randomUUID()  + ".csv"
    }
  }

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(
      s"writing integrated entities ::   [${config.integratedInputFile}] " +
        s"with parameters currentMerged :: [${config.currentMerged}]" +
        s"with parameters previousMerged:: [${config.previousMerged}]" +
        s"with parameters prevIntegrated:: [${config.previousIntegratedInputFile}]" +
        s"with parameters currentMergedOPR:: [${config.currentMergedOPR}]" +
        s"with parameters auroraCountryCodes:: [${config.auroraCountryCodes}]" +
        s"with parameters targetType:: [${config.targetType}]" +
        s"to outbound export csv file for ACM and DDB.")

    if (!config.auroraCountryCodes.isEmpty()) {
      val integrated = storage.readFromParquet[DomainType](config.integratedInputFile)

      if (config.outboundLocation.contains("shared")) {
        val location = config.outboundLocation + entityName() + s"/Processed/" + entityName() + ".parquet"
        val exportds = filterByCountry(integrated, None, spark)
        storage.writeToParquet(exportds, location)
      }
      else {
        config.auroraCountryCodes.split(";").foreach {
          country =>
            val location = config.outboundLocation + country.toLowerCase + "/" + entityName() + s"/Processed/" + entityName() + ".parquet"
            val exportds = filterByCountry(integrated, Some(country), spark)
            storage.writeToParquet(exportds, location)
        }
      }
    } else {
      val previousIntegratedFile = config.previousIntegratedInputFile.fold(spark.createDataset[DomainType](Nil))(storage.readFromParquet[DomainType](_))
      export(storage.readFromParquet[DomainType](config.integratedInputFile), previousIntegratedFile, config, spark)
    }
  }

  /** Get Different Rows between two datasets */
  def getDifferentRows(spark: SparkSession, current: Dataset[DomainType], previous: Dataset[DomainType]): Dataset[DomainType] = {
    if (onlyExportChangedRows) filterOnlyChangedRows(current, previous, spark) else current
  }

  /** Applies a set of pre-processing steps
    *
    * Transformations applied:
    *  - filter golden records for (ACM only)
    *  - filter on selected countries
    *  - entity-specific transformations (implemented in subclass)
    *
    * @param dataset the current dataset
    */
  private def preProcess(spark: SparkSession, config: OutboundConfig, dataset: Dataset[DomainType]) = {
    import spark.implicits._

    val domainEntities = config.targetType match {
      case ACM ⇒ goldenRecordOnlyFilter(spark, dataset).filter(!$"countryCode".isin(config.excludeCountryCodes.split(";"): _*))
      case DDL ⇒ dataset.filter($"countryCode".isin(config.auroraCountryCodes.split(";"):_*)).where($"ohubUpdated".between(config.fromDate, config.toDate))
      case _ ⇒ dataset
    }

    val filteredByCountries =
      if (config.countryCodes.isDefined) {
        domainEntities.filter($"countryCode".isin(config.countryCodes.get: _*))
      }
      else {
        domainEntities
      }

    entitySpecificFilter(spark, filteredByCountries, config)
  }

  def commonTransform(integrated: Dataset[DomainType], previousIntegrated: Dataset[DomainType], config: OutboundConfig, spark: SparkSession
                     ): Dataset[DomainType] = {
    val preProcessedDataset: Dataset[DomainType] = preProcess(spark, config, integrated)
    getDifferentRows(spark, preProcessedDataset, previousIntegrated)
  }


  def filterByCountry(
                       currentIntegrated: Dataset[DomainType],
                       country: Option[String],
                       spark: SparkSession
                     ): Dataset[DomainType] = {
    import spark.implicits._

    if (country.isDefined) {
      currentIntegrated.filter($"countryCode" === country.get)
    }
    else {
      currentIntegrated
    }
  }

  def export(
              currentIntegrated: Dataset[DomainType],
              previousIntegrated: Dataset[DomainType],
              config: OutboundConfig,
              spark: SparkSession
            ) {
    import spark.implicits._

    val deltaIntegrated = commonTransform(currentIntegrated, previousIntegrated, config, spark)

    val columnsInOrder = currentIntegrated.columns
    val result = deltaIntegrated
      .select(columnsInOrder.head, columnsInOrder.tail: _*)
      .as[DomainType]

    if (config.mappingOutputLocation.isDefined && explainConversion.isDefined && currentIntegrated.head(1).nonEmpty) {
      log.info(s"ConversionExplanation found, writing output to ${config.mappingOutputLocation.get}")
      val mapping = explainConversion.get.apply(currentIntegrated.head)
      writeToJson(spark, new Path(config.mappingOutputLocation.get), deserializeJsonFields(mapping))
    }

    val outputDataset = filterValid(spark, convertDataSet(spark, result), config)
    writeToCsv(config, outputDataset, spark)
  }

  def exportToDdl(
                   currentIntegrated: Dataset[DomainType],
                   config: OutboundConfig,
                   spark: SparkSession
                 ) {
    import spark.implicits._

    val preProcessedDataset: Dataset[DomainType] = preProcess(spark, config, currentIntegrated)

    val columnsInOrder = currentIntegrated.columns
    val result = preProcessedDataset
      .select(columnsInOrder.head, columnsInOrder.tail: _*)
      .as[DomainType]

    if (config.mappingOutputLocation.isDefined && explainConversion.isDefined && currentIntegrated.head(1).nonEmpty) {
      log.info(s"ConversionExplanation found, writing output to ${config.mappingOutputLocation.get}")
      val mapping = explainConversion.get.apply(currentIntegrated.head)
      writeToJson(spark, new Path(config.mappingOutputLocation.get), deserializeJsonFields(mapping))
    }

    val outputDataset = filterValid(spark, convertDataSet(spark, result), config)

    writeToCsvInDdl(config, outputDataset, spark)
  }

  def export(
              currentIntegrated: Dataset[DomainType],
              processedChanged: Dataset[DomainType] => Dataset[DomainType],
              currentMerged: Dataset[DomainType],
              previousMerged: Dataset[DomainType],
              currentMergedOPR: DataFrame,
              config: OutboundConfig,
              spark: SparkSession
            ) {
    import spark.implicits._

    val deltaIntegrated = commonTransform(currentMerged, previousMerged, config, spark)
    val filtered = filterValid(spark, deltaIntegrated, config).as[DomainType]
    val processedChangedDS = processedChanged(filtered)
    val operatorLinking = linkOperator(spark, currentMergedOPR, processedChangedDS).as[DomainType]

    val columnsInOrder = currentIntegrated.columns
    val result = operatorLinking
      .select(columnsInOrder.head, columnsInOrder.tail: _*)
      .as[DomainType]

    if (config.mappingOutputLocation.isDefined && explainConversion.isDefined && currentIntegrated.head(1).nonEmpty) {
      log.info(s"ConversionExplanation found, writing output to ${config.mappingOutputLocation.get}")
      val mapping = explainConversion.get.apply(currentIntegrated.head)
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
      "ingestionErrors",
      "lastModifiedDate"
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


  def writeToCsvInDdl(config: OutboundConfig, ds: Dataset[_], sparkSession: SparkSession): Unit = {
    val outputFolderPath = new Path(config.outboundLocation)
    val outputFilePath = new Path(outputFolderPath, filename(config.targetType))

    val rowSize = getBytes(ds.head(1))
    val rowCount = ds.count()
    val partitionSize = 60000000
    val noPartitions: Int = (rowSize * rowCount / partitionSize).toInt

    if (noPartitions.equals(0)) {
      val writeData = ds.write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .option("quoteAll", "true")
        .option("delimiter", ";")
        .option("encoding", "UTF-8")
      writeData.csv(outputFilePath.toString + ".csv")

    } else {
      val writeableData = ds.repartition(noPartitions).write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .option("quoteAll", "true")
        .option("delimiter", ";")
        .option("encoding", "UTF-8")
      writeableData.csv(outputFilePath.toString + ".csv")
    }
  }

  def getBytes(value: Any): Long = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    stream.toByteArray.length
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

  /*
    This method is used only for Contactperson and Operators entity to send the deleted OHubIDs to ACM
   */
  private[export] def getDeletedOhubIdsWithTargetId(
                                                     spark: SparkSession,
                                                     prevIntegratedDS: Dataset[DomainType],
                                                     integratedDS: Dataset[DomainType],
                                                     prevMergedDS: Dataset[DomainType],
                                                     currMergedDS: Dataset[DomainType]
                                                   ) = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val appendTargetOhubIdToAdditionalFields = (additionalFieldsMap: Map[String, String], targetOhubId: String) =>
      additionalFieldsMap + ("targetOhubId" -> targetOhubId)
    val setAdditionalFields = udf(appendTargetOhubIdToAdditionalFields)

    // Fetch the ohubid that changed group
    val deletedOhubIdDataset = prevIntegratedDS
      .filter($"isGoldenRecord")
      .join(integratedDS.filter($"isGoldenRecord"), Seq("ohubId"), "left_anti")
      .withColumn("isActive", lit(false))

    val deletedOhubIdList = deletedOhubIdDataset.select("ohubId").map(r => r(0).toString).collect.toList

    // Set the target_ohub_id
    val groupChange = deletedOhubIdDataset
      .join(integratedDS, Seq("concatId"), "left")
      .select(deletedOhubIdDataset("*"), integratedDS("ohubId") as ("targetOhubId"))
      .withColumn("additionalFields", setAdditionalFields($"additionalFields", $"targetOhubId"))
      .drop("targetOhubId")
      .as[DomainType]

    // Get the remaining ohubids to be deleted because have been disabled
    val disabled_ohubids = prevMergedDS.select("ohubId")
      .except(currMergedDS.select("ohubId"))
      .filter(!$"ohubId".isin(deletedOhubIdList: _*))
      .map(r => r(0).toString).collect.toList

    val disabled = prevMergedDS
      .filter($"ohubId".isin(disabled_ohubids: _*))
      .withColumn("isActive", lit(false))
      .as[DomainType]

    disabled.union(groupChange.select(disabled.columns.head, disabled.columns.tail: _*).as[DomainType])

  }

  /*
    This method is used only for Contactperson and Operators entity to send the deleted OHubIDs to DBB
   */
  private[export] def getDeletedOhubIdsWithTargetIdDBB(
                                                        spark: SparkSession,
                                                        prevIntegratedDS: Dataset[DomainType],
                                                        integratedDS: Dataset[DomainType],
                                                        prevMergedDS: Dataset[DomainType],
                                                        currMergedDS: Dataset[DomainType]
                                                      ) = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // Fetch the ohubid that changed group
    val deletedOhubIdDataset = prevIntegratedDS
      .filter($"isGoldenRecord")
      .join(integratedDS.filter($"isGoldenRecord"), Seq("ohubId"), "left_anti")
      .withColumn("isActive", lit(false)).withColumn("isGoldenRecord", lit(false))

    val deletedOhubIdList = deletedOhubIdDataset.select("ohubId").map(r => r(0).toString).collect.toList

    // Set the target_ohub_id
    val groupChange = deletedOhubIdDataset
      .join(integratedDS, Seq("concatId"), "left")
      .select(deletedOhubIdDataset("*"))
      .as[DomainType]

    // Get the remaining ohubids to be deleted because have been disabled
    val disabled_ohubids = prevMergedDS.select("ohubId")
      .except(currMergedDS.select("ohubId"))
      .filter(!$"ohubId".isin(deletedOhubIdList: _*))
      .map(r => r(0).toString).collect.toList

    val disabled = prevMergedDS
      .filter($"ohubId".isin(deletedOhubIdList ++ disabled_ohubids: _*))
      .withColumn("isActive", lit(false))
      .as[DomainType]

    disabled.union(groupChange.select(disabled.columns.head, disabled.columns.tail: _*).as[DomainType])

  }
}
