package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object OrderLineDomainExportWriter extends DomainExportWriter[OrderLine]

object OrderLine extends DomainEntityCompanion {
  val customerType = "ORDERLINE"
  override val engineFolderName = "orderlines"
  override val domainExportWriter: Option[DomainExportWriter[OrderLine]] = Some(OrderLineDomainExportWriter)
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
