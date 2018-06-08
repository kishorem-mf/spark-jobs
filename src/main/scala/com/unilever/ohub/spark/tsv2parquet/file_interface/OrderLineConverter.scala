package com.unilever.ohub.spark.tsv2parquet.file_interface

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.{OrderLine, Product}
import com.unilever.ohub.spark.tsv2parquet.{DomainTransformer, ProductEmptyParquetWriter}
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import org.apache.spark.sql.Row

object OrderLineConverter extends FileDomainGateKeeper[OrderLine] with ProductEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ OrderLine= { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val concatId: String = createConcatId("COUNTRY_CODE", "SOURCE", "REF_ORDER_ID")
    val orderConcatId: String = createConcatId("COUNTRY_CODE", "SOURCE", "REF_ORDER_ID")
    val ohubCreated = currentTimestamp()

    // format: OFF             // see also: https://stackoverflow.com/questions/3375307/how-to-disable-code-formatting-for-some-part-of-the-code-using-comments

    // fieldName                        mandatory   sourceFieldName             targetFieldName           transformationFunction (unsafe)
    OrderLine(
      concatId                        = concatId,
      countryCode                     = mandatory( "COUNTRY_CODE",              "countryCode"                                       ),
      customerType                    = Product.customerType                                                                         ,
      dateCreated                     = optional(  "DATE_CREATED",              "dateCreated",            parseDateTimeStampUnsafe  ),
      dateUpdated                     = optional(  "DATE_MODIFIED",             "dateUpdated",            parseDateTimeStampUnsafe  ),
      isActive                        = mandatory( "STATUS",                    "isActive",               parseBoolUnsafe           ),
      isGoldenRecord                  = true,
      ohubId                          = Some(UUID.randomUUID().toString),
      sourceEntityId                  = mandatory( "REF_ORDER_ID",            "sourceEntityId"),
      sourceName                      = mandatory( "SOURCE",                    "sourceName"),
      ohubCreated                     = ohubCreated,
      ohubUpdated                     = ohubCreated,
      // specific fields
      orderConcatId                   = orderConcatId,
      quantityOfUnits                 = optional("QUANTITY", "quantityOfUnits", parseLongRangeOption).get,
      amount                          = optional("ORDER_LINE_VALUE", "amount", parseBigDecimalUnsafe),
      pricePerUnit                    = optional("UNIT_PRICE", "pricePerUnit", parseBigDecimalUnsafe),
      currency                        = optional("CURRENCY_CODE", "currency"),
      // other fields
      additionalFields                = additionalFields,
      ingestionErrors                 = errors
    )
    // format: ON
  }
}
