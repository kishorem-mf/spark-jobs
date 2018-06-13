package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.constraint.ConcatIdConstraintTypes.SourceEntityId

object Order {
  val customerType = "ORDER"
  val typeEnum = Set("MERCHANDISE", "DIRECT", "SAMPLE", "EVENT", "SSD", "TRANSFER", "WEB", "BIN", "OTHER", "WEBSHOP", "APPSHOP", "UNKNOWN")
}

case class Order(
    // generic fields
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
    operatorConcatId: String,
    operatorOhubId: Option[String],
    transactionDate: Timestamp,
    vat: Option[BigDecimal],
    // other fields
    additionalFields: Map[String, String],
    ingestionErrors: Map[String, IngestionError]
) extends DomainEntity {
}
