package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.OrderLine
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{ DomainTransformer, OrderLineEmptyParquetWriter }
import org.apache.spark.sql.Row

object OrderLineConverter extends CommonDomainGateKeeper[OrderLine] with OrderLineEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ OrderLine = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val orderLineConcatId: String = createConcatId("countryCode", "sourceName", "sourceEntityId")(row)
    val orderConcatId: String = createConcatId("countryCode", "sourceName", "orderRefId")
    val productConcatId: String = createConcatId("countryCode", "sourceName", "productRefId")
    val ohubCreated = currentTimestamp()

    // format: OFF             // see also: https://stackoverflow.com/questions/3375307/how-to-disable-code-formatting-for-some-part-of-the-code-using-comments

    OrderLine(
      concatId                        = orderLineConcatId,
      countryCode                     = mandatory( "countryCode",              "countryCode"                                    ),
      customerType                    = OrderLine.customerType                                                                   ,
      dateCreated                     = optional(  "dateCreated",              "dateCreated",            parseDateTimeUnsafe()  ),
      dateUpdated                     = optional(  "dateUpdated",              "dateUpdated",            parseDateTimeUnsafe()  ),
      isActive                        = true,
      isGoldenRecord                  = false, // set in OrderLineMerging
      ohubId                          = None, // set in OrderLineMerging
      sourceEntityId                  = mandatoryValue("sourceEntityId",       "sourceEntityId")(row),
      sourceName                      = mandatory( "sourceName",               "sourceName"),
      ohubCreated                     = ohubCreated,
      ohubUpdated                     = ohubCreated,
      // specific fields
      orderConcatId                   = orderConcatId,
      productConcatId                 = productConcatId,
      comment                         = optional(  "comment",                  "comment"),
      quantityOfUnits                 = mandatory( "quantityOfUnits",          "quantityOfUnits",        toInt        ),
      amount                          = mandatory( "amount",                   "amount",                 toBigDecimal ),
      pricePerUnit                    = optional(  "pricePerUnit",             "pricePerUnit",           toBigDecimal ),
      currency                        = optional(  "currency",                 "currency"),
      campaignLabel                   = optional(  "campaignLabel",            "campaignLabel"),
      loyaltyPoints                   = optional(  "loyaltyPoints",            "loyaltyPoints" ,         toBigDecimal ),
      productOhubId                   = None, // set in OrderLineMerging
      // other fields
      additionalFields                = additionalFields,
      ingestionErrors                 = errors
    )
    // format: ON
  }
}