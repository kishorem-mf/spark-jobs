package com.unilever.ohub.spark.ingest.web_event_interface

import java.sql.Timestamp

import cats.syntax.option._
import com.unilever.ohub.spark.domain.entity.Order
import com.unilever.ohub.spark.generic.StringFunctions.checkEnum
import com.unilever.ohub.spark.ingest.CustomParsers.{ parseBigDecimalUnsafe, parseDateTimeStampUnsafe }
import com.unilever.ohub.spark.ingest.{ DomainTransformer, OrderEmptyParquetWriter }
import org.apache.spark.sql.Row

import scala.math.BigDecimal.RoundingMode

object OrderConverter extends WebEventDomainGateKeeper[Order] with OrderEmptyParquetWriter {
  override protected def toDomainEntity: DomainTransformer ⇒ Row ⇒ Order = { transformer ⇒ implicit row ⇒
    import transformer._

    val concatId: String = createConcatId("countryCode", "sourceId", "refOrderId")
    val ohubCreated: Timestamp = currentTimestamp()

    // format: OFF             // see also: https://stackoverflow.com/questions/3375307/how-to-disable-code-formatting-for-some-part-of-the-code-using-comments

    Order(
      concatId                        = concatId,
      countryCode                     = mandatory( "countryCode",           "countryCode"                                       ),
      customerType                    = Order.customerType,
      dateCreated                     = none,
      dateUpdated                     = none,
      isActive                        = true,
      isGoldenRecord                  = false,
      ohubId                          = none,
      sourceEntityId                  = mandatory( "refOrderId",           "sourceEntityId"),
      sourceName                      = SourceName,
      ohubCreated                     = ohubCreated,
      ohubUpdated                     = ohubCreated,

      // specific fields
      `type`                          = mandatory( "type",                  "type",                  checkEnum(Order.typeEnum) _),
      campaignCode                    = optional(  "campaignCode",          "campaignCode"),
      campaignName                    = optional(  "campaignName",          "campaignName"),
      comment                         = optional(  "comments",              "comment"),
      contactPersonConcatId           = none,
      contactPersonOhubId             = none,
      distributorId                   = optional(  "wholesalerId",            "distributorId"),
      distributorLocation             = optional(  "wholesalerLocation",      "distributorLocation"),
      distributorName                 = optional(  "wholesaler",              "distributerName"),
      distributorOperatorId           = optional(  "wholesalerCustomerNumber","distributorOperatorId"),
      operatorConcatId                = concatId,
      operatorOhubId                  = none,
      transactionDate                 = mandatory( "transactionDate",        "transactionDate",       parseDateTimeStampUnsafe),
      vat                             = mandatory( "vat",                    "vat",                   parseBigDecimalUnsafe(_).setScale(2, RoundingMode.HALF_DOWN)),

      // invoice address
      invoiceOperatorName                 = optional("invoiceAddress.name",        "invoiceOperatorName"),
      invoiceOperatorStreet               = optional("invoiceAddress.street",      "invoiceOperatorStreet"),
      invoiceOperatorHouseNumber          = optional("invoiceAddress.houseNumber", "invoiceOperatorHouseNumber"),
      invoiceOperatorHouseNumberExtension = optional("invoiceAddress.houseNumberExtension", "invoiceOperatorHouseNumberExtension"),
      invoiceOperatorZipCode              = optional("invoiceAddress.postCode",    "invoiceOperatorZipCode"),
      invoiceOperatorCity                 = optional("invoiceAddress.city",        "invoiceOperatorCity"),
      invoiceOperatorState                = optional("invoiceAddress.state",       "invoiceOperatorState"),
      invoiceOperatorCountry              = optional("invoiceAddress.country",     "invoiceOperatorCountry"),

      // delivery address
      deliveryOperatorName                 = optional("deliveryAddress.name", "deliveryOperatorName"),
      deliveryOperatorStreet               = optional("deliveryAddress.street", "deliveryOperatorStreet"),
      deliveryOperatorHouseNumber          = optional("deliveryAddress.houseNumber", "deliveryOperatorHouseNumber"),
      deliveryOperatorHouseNumberExtension = optional("deliveryAddress.houseNumberExtension", "deliveryOperatorHouseNumberExtension"),
      deliveryOperatorZipCode              = optional("deliveryAddress.postCode", "deliveryOperatorZipCode"),
      deliveryOperatorCity                 = optional("deliveryAddress.city", "deliveryOperatorCity"),
      deliveryOperatorState                = optional("deliveryAddress.state", "deliveryOperatorState"),
      deliveryOperatorCountry              = optional("deliveryAddress.country", "deliveryOperatorCountry"),

      // other fields
      additionalFields                = additionalFields,
      ingestionErrors                 = errors
    )
    // format: ON
  }
}
