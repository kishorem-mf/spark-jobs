package com.unilever.ohub.spark.tsv2parquet.file_interface

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.Order
import com.unilever.ohub.spark.tsv2parquet.{ DomainTransformer, OrderEmptyParquetWriter }
import com.unilever.ohub.spark.generic.StringFunctions._
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import org.apache.spark.sql.Row

object OrderConverter extends FileDomainGateKeeper[Order] with OrderEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Order = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val concatId: String = createConcatId("COUNTRY_CODE", "SOURCE", "REF_ORDER_ID")
    val ohubCreated = currentTimestamp()

    // format: OFF             // see also: https://stackoverflow.com/questions/3375307/how-to-disable-code-formatting-for-some-part-of-the-code-using-comments

    Order(
      concatId                        = concatId,
      countryCode                     = mandatory( "COUNTRY_CODE",           "countryCode"                                       ),
      customerType                    = Order.customerType,
      dateCreated                     = optional(  "DATE_CREATED",           "dateCreated",            parseDateTimeStampUnsafe  ),
      dateUpdated                     = optional(  "DATE_MODIFIED",          "dateUpdated",            parseDateTimeStampUnsafe  ),
      isActive                        = mandatory( "STATUS",                 "isActive",               parseBoolUnsafe           ),
      isGoldenRecord                  = true,
      ohubId                          = None,
      sourceEntityId                  = mandatory( "REF_ORDER_ID",           "sourceEntityId"),
      sourceName                      = mandatory( "SOURCE",                 "sourceName"),
      ohubCreated                     = ohubCreated,
      ohubUpdated                     = ohubCreated,
      // specific fields
      `type`                          = mandatory( "ORDER_TYPE",             "type",                  checkEnum(Order.typeEnum) _),
      campaignCode                    = optional(  "CAMPAIGN_CODE",          "campaignCode"),
      campaignName                    = optional(  "CAMPAIGN_NAME",          "campaignName"),
      comment                         = None,
      contactPersonConcatId           = optional(  "REF_CONTACT_PERSON_ID",  "contactPersonConcatId"),
      contactPersonOhubId             = None,
      distributorId                   = None,
      distributorLocation             = None,
      distributorName                 = optional(  "WHOLESALER",             "distributerName"),
      distributorOperatorId           = None,
      operatorConcatId                = mandatory( "REF_OPERATOR_ID",        "operatorConcatId"),
      operatorOhubId                  = None,
      transactionDate                 = mandatory( "TRANSACTION_DATE",       "transactionDate",       parseDateTimeStampUnsafe),
      vat                             = None,
      // other fields
      additionalFields                = additionalFields,
      ingestionErrors                 = errors
    )
    // format: ON
  }
}
