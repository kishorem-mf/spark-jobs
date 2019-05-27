package com.unilever.ohub.spark.export.acm.model

import com.unilever.ohub.spark.export.OutboundEntity

case class AcmOrder(
    ORDER_ID: String,
    REF_ORDER_ID: String,
    COUNTRY_CODE: String,
    ORDER_TYPE: String,
    CP_LNKD_INTEGRATION_ID: String,
    OPR_LNKD_INTEGRATION_ID: String,
    CAMPAIGN_CODE: String,
    CAMPAIGN_NAME: String,
    WHOLESALER: String,
    WHOLESALER_ID: String = "",
    WHOLESALER_CUSTOMER_NUMBER: String = "",
    WHOLESALER_LOCATION: String = "",
    ORDER_TOKEN: String = "",
    ORDER_EMAIL_ADDRESS: String = "",
    ORDER_PHONE_NUMBER: String = "",
    ORDER_MOBILE_PHONE_NUMBER: String = "",
    TRANSACTION_DATE: String,
    ORDER_AMOUNT: String = "",
    DELIVERY_STREET: String,
    DELIVERY_HOUSENUMBER: String,
    DELIVERY_ZIPCODE: String,
    DELIVERY_CITY: String,
    DELIVERY_STATE: String,
    DELIVERY_COUNTRY: String,
    DELIVERY_PHONE: String = "",
    INVOICE_NAME: String,
    INVOICE_STREET: String,
    INVOICE_HOUSE_NUMBER: String,
    INVOICE_HOUSE_NUMBER_EXT: String,
    INVOICE_ZIPCODE: String,
    INVOICE_CITY: String,
    INVOICE_STATE: String,
    INVOICE_COUNTRY: String,
    COMMENTS: String,
    VAT: String,
    DELETED_FLAG: String,
    ORDER_AMOUNT_CURRENCY_CODE: String) extends OutboundEntity
