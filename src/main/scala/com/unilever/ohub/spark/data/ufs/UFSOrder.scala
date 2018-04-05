package com.unilever.ohub.spark.data.ufs

case class UFSOrder(
    ORDER_ID: String,
    COUNTRY_CODE: Option[String],
    ORDER_TYPE: Option[String],
    CP_LNKD_INTEGRATION_ID: Option[String],
    OPR_LNKD_INTEGRATION_ID: Option[String],
    CAMPAIGN_CODE: Option[String],
    CAMPAIGN_NAME: Option[String],
    WHOLESALER: Option[String],
    ORDER_TOKEN: String,
    TRANSACTION_DATE: Option[String],
    ORDER_AMOUNT: Option[Double],
    ORDER_AMOUNT_CURRENCY_CODE: Option[String],
    DELIVERY_STREET: String,
    DELIVERY_HOUSENUMBER: String,
    DELIVERY_ZIPCODE: String,
    DELIVERY_CITY: String,
    DELIVERY_STATE: String,
    DELIVERY_COUNTRY: String,
    DELIVERY_PHONE: String
)
