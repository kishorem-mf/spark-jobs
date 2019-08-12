package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.TargetType.{MEPS, TargetType}
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, OrderLineDWWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter
import org.apache.spark.sql.{Dataset, SparkSession}

object OrderLineDomainExportWriter extends DomainExportWriter[OrderLine] {
  override def customExportFiltering(spark: SparkSession, dataSet: Dataset[OrderLine], targetType: TargetType) = {
    import spark.implicits._

    targetType match {
      case MEPS => dataSet.filter($"sourceName" =!= "ARMSTRONG")
      case _ => dataSet
    }
  }
}

object OrderLine extends DomainEntityCompanion[OrderLine] {
  val customerType = "ORDERLINE"
  override val engineFolderName = "orderlines"
  override val domainExportWriter: Option[DomainExportWriter[OrderLine]] = Some(OrderLineDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[OrderLine]] = Some(com.unilever.ohub.spark.export.acm.OrderLineOutboundWriter)
  override val dispatchExportWriter: Option[ExportOutboundWriter[OrderLine]] = Some(com.unilever.ohub.spark.export.dispatch.OrderLineOutboundWriter)
  override val azureDwWriter: Option[AzureDWWriter[OrderLine]] = Some(OrderLineDWWriter)
}

case class OrderLine(
                      // generic fields
                      id: String,
                      creationTimestamp: Timestamp,
                      concatId: String,
                      countryCode: String,
                      customerType: String,
                      dateCreated: Option[Timestamp],
                      dateUpdated: Option[Timestamp],
                      isActive: Boolean,
                      isGoldenRecord: Boolean,
                      sourceEntityId: String,
                      sourceName: String,
                      ohubId: Option[String],
                      ohubCreated: Timestamp,
                      ohubUpdated: Timestamp,
                      // specific fields
                      orderConcatId: String,
                      productConcatId: String,
                      productSourceEntityId: String,
                      quantityOfUnits: Int,
                      amount: BigDecimal,
                      pricePerUnit: Option[BigDecimal],
                      currency: Option[String],
                      comment: Option[String],
                      campaignLabel: Option[String],
                      loyaltyPoints: Option[BigDecimal],
                      productOhubId: Option[String],
                      orderType: Option[String],
                      // other fields
                      additionalFields: Map[String, String],
                      ingestionErrors: Map[String, IngestionError]
                    ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion = OrderLine
}
