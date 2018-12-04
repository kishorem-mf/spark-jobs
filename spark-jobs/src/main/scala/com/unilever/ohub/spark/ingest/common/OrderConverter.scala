package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.Order
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{ DomainTransformer, OrderEmptyParquetWriter }
import org.apache.spark.sql.Row

object OrderConverter extends CommonDomainGateKeeper[Order] with OrderEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Order = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val ohubCreated = currentTimestamp()

    // format: OFF             // see also: https://stackoverflow.com/questions/3375307/how-to-disable-code-formatting-for-some-part-of-the-code-using-comments

    Order(
      id                                    = mandatory( "id",                                    "id"),
      creationTimestamp                     = mandatory( "creationTimestamp",                     "creationTimestamp", toTimestamp),
      concatId                              = mandatory( "concatId",                              "concatId"),
      countryCode                           = mandatory( "countryCode",                           "countryCode"                                  ),
      customerType                          = Order.customerType,
      dateCreated                           = optional(  "dateCreated",                           "dateCreated",                            parseDateTimeUnsafe()),
      dateUpdated                           = optional(  "dateUpdated",                           "dateUpdated",                            parseDateTimeUnsafe()),
      isActive                              = mandatory( "isActive",                              "isActive",                               toBoolean            ),
      isGoldenRecord                        = false, // set in OrderMerging
      ohubId                                = None, // set in OrderMerging
      sourceEntityId                        = mandatory( "sourceEntityId",                        "sourceEntityId"),
      sourceName                            = mandatory( "sourceName",                            "sourceName"),
      ohubCreated                           = ohubCreated,
      ohubUpdated                           = ohubCreated,
      // specific fields
      `type`                                = mandatory( "orderType",                             "orderType" ),
      campaignCode                          = optional(  "campaignCode",                          "campaignCode"),
      campaignName                          = optional(  "campaignName",                          "campaignName"),
      comment                               = optional(  "comment",                               "comment"),
      contactPersonConcatId                 = optional(  "contactPersonConcatId",                 "contactPersonConcatId"),
      contactPersonOhubId                   = None, // set in OrderMerging
      distributorId                         = optional(  "distributorId",                         "distributorId"),
      distributorLocation                   = optional(  "distributorLocation",                   "distributorLocation"),
      distributorName                       = optional(  "distributorName",                       "distributorName"),
      distributorOperatorId                 = optional(  "distributorOperatorId",                 "distributorOperatorId"),
      operatorConcatId                      = optional(  "operatorConcatId",                      "operatorConcatId"),
      operatorOhubId                        = None, // set in OrderMerging
      transactionDate                       = mandatory( "transactionDate",                       "transactionDate",                        parseDateTimeUnsafe()),
      vat                                   = optional(  "vat",                                   "vat",                                    toBigDecimal         ),
      // invoice address
      invoiceOperatorName                   = optional(  "invoiceOperatorName",                   "invoiceOperatorName"),
      invoiceOperatorStreet                 = optional(  "invoiceOperatorStreet",                 "invoiceOperatorStreet"),
      invoiceOperatorHouseNumber            = optional(  "invoiceOperatorHouseNumber",            "invoiceOperatorHouseNumber"),
      invoiceOperatorHouseNumberExtension   = optional(  "invoiceOperatorHouseNumberExtension",   "invoiceOperatorHouseNumberExtension"),
      invoiceOperatorZipCode                = optional(  "invoiceOperatorZipCode",                "invoiceOperatorZipCode"),
      invoiceOperatorCity                   = optional(  "invoiceOperatorCity",                   "invoiceOperatorCity"),
      invoiceOperatorState                  = optional(  "invoiceOperatorState",                  "invoiceOperatorState"),
      invoiceOperatorCountry                = optional(  "invoiceOperatorCountry",                "invoiceOperatorCountry"),
      // delivery address
      deliveryOperatorName                  = optional(  "deliveryOperatorName",                  "deliveryOperatorName"),
      deliveryOperatorStreet                = optional(  "deliveryOperatorStreet",                "deliveryOperatorStreet"),
      deliveryOperatorHouseNumber           = optional(  "deliveryOperatorHouseNumber",           "deliveryOperatorHouseNumber"),
      deliveryOperatorHouseNumberExtension  = optional(  "deliveryOperatorHouseNumberExtension",  "deliveryOperatorHouseNumberExtension"),
      deliveryOperatorZipCode               = optional(  "deliveryOperatorZipCode",               "deliveryOperatorZipCode"),
      deliveryOperatorCity                  = optional(  "deliveryOperatorCity",                  "deliveryOperatorCity"),
      deliveryOperatorState                 = optional(  "deliveryOperatorState",                 "deliveryOperatorState"),
      deliveryOperatorCountry               = optional(  "deliveryOperatorCountry",               "deliveryOperatorCountry"),
      // other fields
      additionalFields                      = additionalFields,
      ingestionErrors                       = errors
    )
    // format: ON
  }
}
