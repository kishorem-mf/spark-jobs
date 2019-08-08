package com.unilever.ohub.spark.export.domain

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityHash, DomainEntityUtils}
import com.unilever.ohub.spark.export.TargetType.{DATASCIENCE, MEPS, TargetType}
import com.unilever.ohub.spark.export.{CsvOptions, ExportOutboundWriter, OutboundConfig, SparkJobWithOutboundExportConfig}
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}

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

  private val domainEntityComanion = DomainEntityUtils.domainCompanionOf[DomainType]

  override private[export] def filterDataSet(spark: SparkSession, dataSet: Dataset[DomainType], config: OutboundConfig): Dataset[DomainType] = {
    import spark.implicits._

    DomainExportWriter.countryFilterConfig.get(config.targetType) match {
      case Some(visibleCountryCodes) => dataSet.filter($"countryCode".isin(visibleCountryCodes: _*))
      case _ => dataSet
    }
  }

  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[DomainType]) = {
    dataSet.drop(domainEntityComanion.excludedFieldsForCsvExport: _*)
  }

  override def filename(targetType: TargetType): String = {
    val runId = LocalDateTime.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    targetType match {
      case DATASCIENCE => s"${entityName()}/datascience"
      case MEPS => s"UFS_${MEPS.toString}_${entityName().toUpperCase}_${runId}.csv"
      case _ => throw new IllegalArgumentException("provided targetType not supported")
    }
  }

  /**
    * When running this job, do bear in mind that the locations are now folders, the entity name will be appended to it
    * to determine the location.
    *
    * F.e. to export data from runId "2019-08-06" provide "integratedInputFile" as:
    * "dbfs:/mnt/engine/integrated/2019-08-06"
    * In this case CP will be fetched from:
    * "dbfs:/mnt/engine/integrated/2019-08-06/contactpersons.parquet"
    */
  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage) = {
    import spark.implicits._

    log.info(s"writing integrated entities [${entityName()}] to outbound export csv file for ${config.targetType.toString}")

    val hashesInputFile = config.hashesInputFile match {
      case Some(location) ⇒ {
        val hashLocation = s"${location}/${entityName()}.parquet"
        storage.readFromParquet[DomainEntityHash](hashLocation)
      }
      case None ⇒ spark.createDataset(Seq[DomainEntityHash]())
    }

    val integratedLocation = s"${config.integratedInputFile}/${entityName()}.parquet"

    export(storage.readFromParquet[DomainType](integratedLocation), hashesInputFile, config, spark)
  }

  override def entityName() = domainEntityComanion.engineFolderName
}

/**
  * Runs concrete [[com.unilever.ohub.spark.export.domain.DomainExportWriter]]'s run method for all
  * [[com.unilever.ohub.spark.domain.DomainEntity]]s.
  */
object AllDomainEntitiesWriter extends SparkJobWithOutboundExportConfig {
  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    DomainEntityUtils.domainCompanionObjects
      .par
      .filter(_.domainExportWriter.isDefined)
      .map(_.domainExportWriter.get)
      .foreach(_.run(spark, config, storage))
  }
}