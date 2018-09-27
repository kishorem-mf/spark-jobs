package com.unilever.ohub.spark.dispatcher.model

object DispatcherProducts {

}

case class DispatcherProducts(
    BRAND: Option[String],
    PRD_INTEGRATION_ID: Option[String],
    COUNTRY_CODE: Option[String],
    UNIT_PRICE_CURRENCY: Option[String],
    EAN_CODE: Option[String],
    EAN_CODE_DISPATCH_UNIT: Option[String],
    DELETE_FLAG: Option[String],
    PRODUCT_NAME: Option[String],
    CREATED_AT: Option[String],
    UPDATED_AT: Option[String],
    MRDR_CODE: Option[String],
    SOURCE: Option[String],
    SUB_BRAND: Option[String],
    SUB_CATEGORY: Option[String],
    ITEM_TYPE: Option[String],
    UNIT: Option[String],
    UNIT_PRICE: Option[String],
    CATEGORY: Option[String]
)
