package com.unilever.ohub.spark.export.aurora

import com.unilever.ohub.spark.domain.DomainEntityUtils
import com.unilever.ohub.spark.domain.entity._
import com.unilever.ohub.spark.domain.utils.OperatorRef
import com.unilever.ohub.spark.export.dispatch.{CampaignOpenDispatcherConverter, DispatcherOptions, OperatorDispatchConverter}
import com.unilever.ohub.spark.export.dispatch.model.{DispatchContactPerson, _}
import com.unilever.ohub.spark.export.{CsvOptions, ExportOutboundWriter, OutboundConfig, OutboundEntity, SparkJobWithOutboundExportConfig}
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


/**
 * Runs concrete [[com.unilever.ohub.spark.export.ExportOutboundWriter]]'s run method for all
 * [[com.unilever.ohub.spark.domain.DomainEntity]]s dispatchExportWriter values.
 *
 * When running this job, do bear in mind that the input location is now a folder, the entity name will be appended to it
 * to determine the location.
 *
 * F.e. to export data from runId "2019-08-06" provide "integratedInputFile" as:
 * "dbfs:/mnt/engine/integrated/2019-08-06"
 * In this case CP will be fetched from:
 * "dbfs:/mnt/engine/integrated/2019-08-06/contactpersons.parquet"
 **/

object OperatorOutboundWriter extends ExportOutboundWriter[Operator] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Operator]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[Operator]
  override def entityName(): String = domainEntityCompanion.engineFolderName
 }

object ContactPersonOutboundWriter extends ExportOutboundWriter[ContactPerson] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[ContactPerson]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[ContactPerson]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object ActivityOutboundWriter extends ExportOutboundWriter[Activity] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Activity]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[Activity]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object AnswerOutboundWriter extends ExportOutboundWriter[Answer] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Answer]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[Answer]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object CampaignOutboundWriter extends ExportOutboundWriter[Campaign] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Campaign]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[Campaign]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object CampaignBounceOutboundWriter extends ExportOutboundWriter[CampaignBounce] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignBounce]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[CampaignBounce]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object CampaignClickOutboundWriter extends ExportOutboundWriter[CampaignClick] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignClick]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[CampaignClick]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object CampaignOpenOutboundWriter extends ExportOutboundWriter[CampaignOpen] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignOpen]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[CampaignOpen]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object CampaignSendOutboundWriter extends ExportOutboundWriter[CampaignSend] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignSend]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[CampaignSend]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object ChainOutboundWriter extends ExportOutboundWriter[Chain] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Chain]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[Chain]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object ChannelMappingOutboundWriter extends ExportOutboundWriter[ChannelMapping] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[ChannelMapping]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[ChannelMapping]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object ContactPersonChangeLogOutboundWriter extends ExportOutboundWriter[ContactPersonChangeLog] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[ContactPersonChangeLog]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[ContactPersonChangeLog]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object ContactPersonGoldenOutboundWriter extends ExportOutboundWriter[ContactPersonGolden] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[ContactPersonGolden]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[ContactPersonGolden]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object LoyaltyPointsOutboundWriter extends ExportOutboundWriter[LoyaltyPoints] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[LoyaltyPoints]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[LoyaltyPoints]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object OperatorChangeLogOutboundWriter extends ExportOutboundWriter[OperatorChangeLog] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[OperatorChangeLog]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[OperatorChangeLog]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object OperatorGoldenOutboundWriter extends ExportOutboundWriter[OperatorGolden] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[OperatorGolden]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[OperatorGolden]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object OrderOutboundWriter extends ExportOutboundWriter[Order] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Order]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[Order]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object OrderLineOutboundWriter extends ExportOutboundWriter[OrderLine] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[OrderLine]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[OrderLine]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object ProductOutboundWriter extends ExportOutboundWriter[Product] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Product]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[Product]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object QuestionOutboundWriter extends ExportOutboundWriter[Question] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Question]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[Question]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object SubscriptionOutboundWriter extends ExportOutboundWriter[Subscription] {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Subscription]) = {
    // This is needed because in ExportOutboundWriter convertDataSet and entityName is mandatory
    dataSet
  }
  protected val domainEntityCompanion = DomainEntityUtils.domainCompanionOf[Subscription]
  override def entityName(): String = domainEntityCompanion.engineFolderName
}

object AllAuroraOutboundWriter  extends SparkJobWithOutboundExportConfig  {
  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    DomainEntityUtils.domainCompanionObjects
      .par
      .filter(_.auroraExportWriter.isDefined)
      .foreach(entity => {
        val writer = entity.auroraExportWriter.get
        val integratedLocation = s"${config.integratedInputFile}/${entity.engineFolderName}.parquet"
        val datalakeLocation = if(entity.auroraFolderLocation == Some("Restricted")){s"${config.outboundLocation}Restricted/"}
        else if(entity.auroraFolderLocation == Some("Shared")){s"${config.outboundLocation}shared/"}
        else {s"${config.outboundLocation}/"}

        writer.run(
          spark,
          config.copy(
            integratedInputFile = integratedLocation,
            outboundLocation = datalakeLocation,
            auroraCountryCodes = config.auroraCountryCodes,
            targetType = config.targetType
          ),
          storage)
      }
      )
  }
}

