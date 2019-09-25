package com.unilever.ohub.spark.export.domain

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.unilever.ohub.spark.domain.entity.{ContactPerson, Operator}
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityUtils}
import com.unilever.ohub.spark.export.TargetType.{DATASCIENCE, MEPS, TargetType}
import com.unilever.ohub.spark.export.{CsvOptions, ExportOutboundWriter, OutboundConfig, SparkJobWithOutboundExportConfig}
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.when

import scala.reflect.runtime.universe._

object DomainExportWriter {
  val countryFilterConfig = Seq(
    MEPS -> Seq("PK", "EG", "LK", "AE", "BH", "JO", "KW", "LB", "MV", "OM", "QA", "SA")
  ).toMap[TargetType, Seq[String]]
}

trait DomainExportOptions extends CsvOptions {
  override val delimiter: String = ";"

  override val extraOptions = Map(
    "delimiter" -> delimiter
  )
}

abstract class DomainExportWriter[DomainType <: DomainEntity : TypeTag] extends ExportOutboundWriter[DomainType] with DomainExportOptions {
  override val onlyExportChangedRows: Boolean = false

  override def mergeCsvFiles(targetType: TargetType): Boolean = {
    targetType match {
      case DATASCIENCE => false
      case MEPS => true
      case _ => throw new IllegalArgumentException("provided targetType not supported")
    }
  }

  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[DomainType]

  def customExportFiltering(spark: SparkSession, dataSet: Dataset[DomainType], targetType: TargetType): Dataset[DomainType] = dataSet

  override private[export] def filterDataSet(spark: SparkSession, dataSet: Dataset[DomainType], config: OutboundConfig): Dataset[DomainType] = {
    import spark.implicits._

    val preFiltered = customExportFiltering(spark, dataSet, config.targetType)

    DomainExportWriter.countryFilterConfig.get(config.targetType) match {
      case Some(visibleCountryCodes) => preFiltered.filter($"countryCode".isin(visibleCountryCodes: _*))
      case _ => preFiltered
    }
  }

  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[DomainType]) = {
    dataSet.drop(domainEntityCompanion.excludedFieldsForCsvExport: _*)
  }

  override def filename(targetType: TargetType): String = {
    val runId = LocalDateTime.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    targetType match {
      case DATASCIENCE => s"${entityName()}/datascience"
      case MEPS => s"UFS_${MEPS.toString}_${entityName().toUpperCase}_${runId}.csv"
      case _ => throw new IllegalArgumentException("provided targetType not supported")
    }
  }

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"writing integrated entities [${entityName()}] to outbound export csv file for ${config.targetType.toString}")

    val previousIntegratedFile = config.previousIntegratedInputFile match {
      case Some(location) => storage.readFromParquet[DomainType](location)
      case None => spark.createDataset[DomainType](Seq())
    }

    export(storage.readFromParquet[DomainType](config.integratedInputFile), previousIntegratedFile, config, spark)
  }

  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object OperatorDomainExportWriter extends DomainExportWriter[Operator] {
  override def convertDataSet(spark: SparkSession, dataSet: Dataset[Operator]): DataFrame = {
    import spark.implicits._
    dataSet
      .withColumn("dateUpdated", when($"dateUpdated".isNull, $"dateCreated") otherwise($"dateUpdated"))
      .drop(domainEntityCompanion.excludedFieldsForCsvExport: _*)
  }
}
object ContactPersonDomainExportWriter extends DomainExportWriter[ContactPerson] {
  override def convertDataSet(spark: SparkSession, dataSet: Dataset[ContactPerson]): DataFrame = {
    import spark.implicits._
    dataSet
      .withColumn("dateUpdated", when($"dateUpdated".isNull, $"dateCreated") otherwise($"dateUpdated"))
      .drop(domainEntityCompanion.excludedFieldsForCsvExport: _*)
  }
}

/**
 * Runs concrete [[com.unilever.ohub.spark.export.domain.DomainExportWriter]]'s run method for all
 * [[com.unilever.ohub.spark.domain.DomainEntity]]s.
 *
 * When running this job, do bear in mind that the input location is now a folder, the entity name will be appended to it
 * to determine the location.
 *
 * F.e. to export data from runId "2019-08-06" provide "integratedInputFile" as:
 * "dbfs:/mnt/engine/integrated/2019-08-06"
 * In this case CP will be fetched from:
 * "dbfs:/mnt/engine/integrated/2019-08-06/contactpersons.parquet"
 **/
object AllDomainEntitiesWriter extends SparkJobWithOutboundExportConfig {
  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    DomainEntityUtils.domainCompanionObjects
      .par
      .filter(_.domainExportWriter.isDefined)
      .foreach((entity) => {
        val writer = entity.domainExportWriter.get
        val integratedLocation = s"${config.integratedInputFile}/${entity.engineFolderName}.parquet"
        val previousIntegratedLocation = if (config.previousIntegratedInputFile.isDefined) Some(s"${config.previousIntegratedInputFile.get}/${entity.engineFolderName}.parquet") else None
        writer.run(spark, config.copy(
          integratedInputFile = integratedLocation,
          previousIntegratedInputFile = previousIntegratedLocation
        ), storage)
      })
  }
}
