package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.TargetType.{MEPS, TargetType}
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, OrderDWWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter
import org.apache.spark.sql.{Dataset, SparkSession}

object OrderDomainExportWriter extends DomainExportWriter[Order] {
  override def customExportFiltering(spark: SparkSession, dataSet: Dataset[Order], targetType: TargetType) = {
    import spark.implicits._

    targetType match {
      case MEPS => dataSet.filter($"sourceName" =!= "ARMSTRONG")
      case _ => dataSet
    }
  }
}

object Order extends DomainEntityCompanion[Order] {
  val customerType = "ORDER"
  override val engineFolderName = "orders"
  override val domainExportWriter: Option[DomainExportWriter[Order]] = Some(OrderDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[Order]] = Some(com.unilever.ohub.spark.export.acm.OrderOutboundWriter)
  override val dispatchExportWriter: Option[ExportOutboundWriter[Order]] = Some(com.unilever.ohub.spark.export.dispatch.OrderOutboundWriter)
  override val azureDwWriter: Option[AzureDWWriter[Order]] = Some(OrderDWWriter)
}

case class Order(
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
                  orderUid: Option[String],
                  `type`: String,
                  campaignCode: Option[String],
                  campaignName: Option[String],
                  comment: Option[String],
                  contactPersonConcatId: Option[String],
                  contactPersonOhubId: Option[String],
                  distributorId: Option[String],
                  distributorLocation: Option[String],
                  distributorName: Option[String],
                  distributorOperatorId: Option[String],
                  currency: Option[String],
                  operatorConcatId: Option[String],
                  operatorOhubId: Option[String],
                  transactionDate: Option[Timestamp],
                  vat: Option[BigDecimal],
                  amount: Option[BigDecimal],
                  // invoice address
                  invoiceOperatorName: Option[String],
                  invoiceOperatorStreet: Option[String],
                  invoiceOperatorHouseNumber: Option[String],
                  invoiceOperatorHouseNumberExtension: Option[String],
                  invoiceOperatorZipCode: Option[String],
                  invoiceOperatorCity: Option[String],
                  invoiceOperatorState: Option[String],
                  invoiceOperatorCountry: Option[String],
                  // delivery address
                  deliveryOperatorName: Option[String],
                  deliveryOperatorStreet: Option[String],
                  deliveryOperatorHouseNumber: Option[String],
                  deliveryOperatorHouseNumberExtension: Option[String],
                  deliveryOperatorZipCode: Option[String],
                  deliveryOperatorCity: Option[String],
                  deliveryOperatorState: Option[String],
                  deliveryOperatorCountry: Option[String],
                  // other fields
                  additionalFields: Map[String, String],
                  ingestionErrors: Map[String, IngestionError]
                ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[Order] = Order
}
