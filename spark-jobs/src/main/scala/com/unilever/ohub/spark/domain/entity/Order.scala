package com.unilever.ohub.spark.domain.entity

import java.sql.{Date, Timestamp}

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.TargetType.{MEPS, TargetType}
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, OrderDWWriter}
import com.unilever.ohub.spark.export.businessdatalake.{AzureDLWriter, OrderDLWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{Dataset, SparkSession}

object OrderDomainExportWriter extends DomainExportWriter[Order] {
  override def customExportFiltering(spark: SparkSession, dataSet: Dataset[Order], targetType: TargetType): Dataset[Order] = {
    import spark.implicits._

    targetType match {
      case MEPS => dataSet.filter($"sourceName" =!= "ARMSTRONG")
      case _ => dataSet
    }
  }
}

object Order extends DomainEntityCompanion[Order] {
  val customerType = "ORDER"
  override val auroraFolderLocation = Some("Restricted")
  override val engineFolderName = "orders"
  override val domainExportWriter: Option[DomainExportWriter[Order]] = Some(OrderDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[Order]] = Some(com.unilever.ohub.spark.export.acm.OrderOutboundWriter)
  override val dispatchExportWriter: Option[ExportOutboundWriter[Order]] = Some(com.unilever.ohub.spark.export.dispatch.OrderOutboundWriter)
  override val azureDwWriter: Option[AzureDWWriter[Order]] = Some(OrderDWWriter)
  override val auroraInboundWriter: Option[ExportOutboundWriter[Order]] = Some(com.unilever.ohub.spark.datalake.OrderOutboundWriter)
  override val dataLakeWriter: Option[AzureDLWriter[Order]] = Some(OrderDLWriter)
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
                  ufsClientNumber: Option[String],
                  deliveryType: Option[String],
                  preferredDeliveryDateOption: Option[String],
                  preferredDeliveryDate: Option[Timestamp],
                  department: Option[String],
                  //BDL Fields
                  createdBy : Option[String],
                  createdBySap : Option[Boolean],
                  customerHierarchyLevel3 : Option[String],
                  customerHierarchyLevel4 : Option[String],
                  customerHierarchyLevel5 : Option[String],
                  customerHierarchyLevel6 : Option[String],
                  customerNameLevel3 : Option[String],
                  customerNameLevel4 : Option[String],
                  customerNameLevel5 : Option[String],
                  customerNameLevel6 : Option[String],
                  deliveryStatus : Option[String],
                  discount : Option[Decimal],
                  externalCustomerHierarchyLevel1 : Option[String],
                  externalCustomerHierarchyLevel1Description : Option[String],
                  externalCustomerHierarchyLevel2 : Option[String],
                  externalCustomerHierarchyLevel2Description : Option[String],
                  invoiceNumber : Option[String],
                  netInvoiceValue : Option[Decimal],
                  opportunityOwner : Option[String],
                  orderCreationDate : Option[Timestamp],
                  purchaseOrderNumber : Option[String],
                  purchaseOrderType : Option[String],
                  rejectionStatus : Option[String],
                  sellInOrSellOut : Option[String],
                  stageOfCompletion : Option[String],
                  totalGrossPrice : Option[Decimal],
                  totalSurcharge : Option[Decimal],
                  wholesalerDistributionCenter : Option[String],
                  wholesalerSellingPriceBasedAmount : Option[Decimal],
                  // other fields
                  additionalFields: Map[String, String],
                  ingestionErrors: Map[String, IngestionError]
                ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[Order] = Order
}
