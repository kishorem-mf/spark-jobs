package com.unilever.ohub.spark.data

import java.sql.Timestamp

case class ProductRecord(
    productConcatId: String,
    refProductId: Option[String],
    source: Option[String],
    countryCode: Option[String],
    status: Option[Boolean],
    statusOriginal: Option[String],
    dateCreated: Option[Timestamp],
    dateCreatedOriginal: Option[String],
    dateModified: Option[Timestamp],
    dateModifiedOriginal: Option[String],
    productName: Option[String],
    eanCu: Option[String],
    eanDu: Option[String],
    mrdr: Option[String],
    unit: Option[String],
    unitPrice: Option[BigDecimal],
    unitPriceOriginal: Option[String],
    unitPriceCurrency: Option[String]
)
