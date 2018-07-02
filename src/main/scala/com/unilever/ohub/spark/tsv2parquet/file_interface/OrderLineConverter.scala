package com.unilever.ohub.spark.tsv2parquet.file_interface

import java.util.UUID

import scala.math.BigDecimal.RoundingMode
import com.unilever.ohub.spark.domain.entity.{ OrderLine, Product }
import com.unilever.ohub.spark.tsv2parquet.{ DomainTransformer, OrderLineEmptyParquetWriter }
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import org.apache.spark.sql.Row

object OrderLineConverter extends FileDomainGateKeeper[OrderLine] with OrderLineEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ OrderLine = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val orderLineConcatId: String = createConcatId("COUNTRY_CODE", "SOURCE", "REF_ORDER_ID")(row) + "_" + mandatoryValue("REF_PRODUCT_ID", "orderLineConcatId")(row)
    val orderConcatId: String = createConcatId("COUNTRY_CODE", "SOURCE", "REF_ORDER_ID")
    val productConcatId: String = createConcatId("COUNTRY_CODE", "SOURCE", "REF_PRODUCT_ID")
    val ohubCreated = currentTimestamp()

    // format: OFF             // see also: https://stackoverflow.com/questions/3375307/how-to-disable-code-formatting-for-some-part-of-the-code-using-comments

    OrderLine(
      concatId                        = orderLineConcatId,
      countryCode                     = mandatory( "COUNTRY_CODE",              "countryCode"                                       ),
      customerType                    = OrderLine.customerType                                                                         ,
      dateCreated                     = optional(  "DATE_CREATED",              "dateCreated",            parseDateTimeStampUnsafe  ),
      dateUpdated                     = optional(  "DATE_MODIFIED",             "dateUpdated",            parseDateTimeStampUnsafe  ),
      isActive                        = true,
      isGoldenRecord                  = true,
      ohubId                          = None, // set in OrderLineMerging
      sourceEntityId                  = mandatory( "REF_ORDER_ID",              "sourceEntityId"),
      sourceName                      = mandatory( "SOURCE",                    "sourceName"),
      ohubCreated                     = ohubCreated,
      ohubUpdated                     = ohubCreated,
      // specific fields
      orderConcatId                   = orderConcatId,
      productConcatId                 = productConcatId,
      comment                         = None,
      quantityOfUnits                 = mandatory( "QUANTITY",                  "quantityOfUnits",        parseLongRangeOption(_).get),
      amount                          = mandatory( "ORDER_LINE_VALUE",          "amount",                 parseBigDecimalUnsafe(_).setScale(2, RoundingMode.HALF_DOWN)),
      pricePerUnit                    = optional(  "UNIT_PRICE",                "pricePerUnit",           parseBigDecimalUnsafe(_).setScale(2, RoundingMode.HALF_DOWN)),
      currency                        = optional(  "CURRENCY_CODE",             "currency"),
      campaignLabel                   = None,
      loyaltyPoints                   = None,
      productOhubId                   = None,
      // other fields
      additionalFields                = additionalFields,
      ingestionErrors                 = errors
    )
    // format: ON
  }
}
