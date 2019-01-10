package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError

object Order {
  val customerType = "ORDER"
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
    orderUID: Option[String],
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
    operatorConcatId: Option[String],
    operatorOhubId: Option[String],
    transactionDate: Option[Timestamp],
    vat: Option[BigDecimal],
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
) extends DomainEntity
