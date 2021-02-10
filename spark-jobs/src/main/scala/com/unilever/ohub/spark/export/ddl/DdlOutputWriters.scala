package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.DomainEntityUtils
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.export._
import com.unilever.ohub.spark.export.ddl.model.DdlOperator
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}

trait DdlOptions extends CsvOptions {

  override val delimiter: String = ";"

  override val extraOptions = Map(
    "delimiter" -> delimiter
  )

}

object OperatorDdlOutboundWriter extends ExportOutboundWriter[Operator] with DdlOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Operator]) = {
    import spark.implicits._
    dataSet.map(OperatorDdlConverter.convert(_))
  }

  override def explainConversion: Option[Operator => DdlOperator] = Some((input: Operator) => OperatorDdlConverter.convert(input, true))

  override def entityName(): String = "OPERATORS"

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {

    val currentIntegrated = storage.readFromParquet[Operator](config.integratedInputFile)

    exportToDdl(
      currentIntegrated,
      config.copy(targetType = TargetType.DDL),
      spark
    )
  }

}

object AllDdlOutboundWriter extends SparkJobWithOutboundExportConfig {
  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    DomainEntityUtils.domainCompanionObjects
      .par
      .filter(_.ddlExportWriter.isDefined)
      .foreach(entity => {
        val writer = entity.ddlExportWriter.get
        val integratedLocation = s"${config.integratedInputFile}/${entity.engineFolderName}.parquet"
        val mappingOutputLocation1 = config.mappingOutputLocation.map(mappingOutboundLocation => s"${mappingOutboundLocation}/${config.targetType}_${writer.entityName()}_MAPPING.json")
        writer.run(
          spark,
          config.copy(
            integratedInputFile = integratedLocation,
            outboundLocation = config.outboundLocation,
            auroraCountryCodes = config.auroraCountryCodes,
            fromDate = config.fromDate,
            toDate = config.toDate,
            sourceName = config.sourceName,
            mappingOutputLocation = mappingOutputLocation1),
          storage)
      })
  }
}
