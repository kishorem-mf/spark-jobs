package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import cats.syntax.option._

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
    // invoice address
    invoiceOperatorName: Option[String] = none,
    invoiceOperatorStreet: Option[String] = none,
    invoiceOperatorHouseNumber: Option[String] = none,
    invoiceOperatorHouseNumberExtension: Option[String] = none,
    invoiceOperatorZipCode: Option[String] = none,
    invoiceOperatorCity: Option[String] = none,
    invoiceOperatorState: Option[String] = none,
    invoiceOperatorCountry: Option[String] = none,
    // delivery address
    deliveryOperatorName: Option[String] = none,
    deliveryOperatorStreet: Option[String] = none,
    deliveryOperatorHouseNumber: Option[String] = none,
    deliveryOperatorHouseNumberExtension: Option[String] = none,
    deliveryOperatorZipCode: Option[String] = none,
    deliveryOperatorCity: Option[String] = none,
    deliveryOperatorState: Option[String] = none,
    deliveryOperatorCountry: Option[String] = none,
    // other fields
    additionalFields: Map[String, String],
    ingestionErrors: Map[String, IngestionError]
) extends DomainEntity {
}
