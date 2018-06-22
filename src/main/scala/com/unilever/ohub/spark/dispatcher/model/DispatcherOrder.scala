package com.unilever.ohub.spark.dispatcher
package model

import com.unilever.ohub.spark.domain.entity.Order

object DispatcherOrder {
  def fromOrder(order: Order): DispatcherOrder = {
    DispatcherOrder(
      CAMPAIGN_CODE = order.campaignCode,
      CAMPAIGN_NAME = order.campaignName,
      COMMENTS = order.comment,
      ORD_INTEGRATION_ID = order.concatId,
      CP_ORIG_INTEGRATION_ID = order.contactPersonConcatId,
      COUNTRY_CODE = order.countryCode,
      WHOLESALER_ID = order.distributorId,
      WHOLESALER_LOCATION = order.distributorLocation,
      WHOLESALER = order.distributorName,
      WHOLESALER_CUSTOMER_NUMBER = order.distributorOperatorId,
      DELETED_FLAG = order.isActive.invert,
      CREATED_AT = order.ohubCreated.mapWithDefaultPattern,
      UPDATED_AT = order.ohubUpdated.mapWithDefaultPattern,
      OPR_ORIG_INTEGRATION_ID = order.operatorConcatId,
      SOURCE_ID = order.sourceEntityId,
      SOURCE = order.sourceName,
      TRANSACTION_DATE = order.transactionDate.mapWithDefaultPattern,
      ORDER_TYPE = order.`type`,
      VAT = order.vat,
      ORDER_EMAIL_ADDRESS = None,
      ORDER_PHONE_NUMBER = None,
      ORDER_MOBILE_PHONE_NUMBER = None,
      TOTAL_VALUE_ORDER_CURRENCY = None,
      TOTAL_VALUE_ORDER_AMOUNT = None,
      ORIGIN = None,
      WS_DC = None,
      ORDER_UID = None,
      DELIVERY_STREET = None,
      DELIVERY_HOUSE_NUMBER = None,
      DELIVERY_HOUSE_NUMBER_EXT = None,
      DELIVERY_POST_CODE = None,
      DELIVERY_CITY = None,
      DELIVERY_STATE = None,
      DELIVERY_COUNTRY = None,
      DELIVERY_PHONE = None,
      INVOICE_NAME = None,
      INVOICE_STREET = None,
      INVOICE_HOUSE_NUMBER = None,
      INVOICE_HOUSE_NUMBER_EXT = None,
      INVOICE_ZIPCODE = None,
      INVOICE_CITY = None,
      INVOICE_STATE = None,
      INVOICE_COUNTRY = None
    )
  }
}

case class DispatcherOrder(
    CAMPAIGN_CODE: Option[String],
    CAMPAIGN_NAME: Option[String],
    COMMENTS: Option[String],
    ORD_INTEGRATION_ID: String,
    CP_ORIG_INTEGRATION_ID: Option[String],
    COUNTRY_CODE: String,
    WHOLESALER_ID: Option[String],
    WHOLESALER_LOCATION: Option[String],
    WHOLESALER: Option[String],
    WHOLESALER_CUSTOMER_NUMBER: Option[String],
    DELETED_FLAG: Boolean,
    CREATED_AT: String,
    UPDATED_AT: String,
    OPR_ORIG_INTEGRATION_ID: String,
    SOURCE_ID: String,
    SOURCE: String,
    TRANSACTION_DATE: String,
    ORDER_TYPE: String,
    VAT: Option[BigDecimal],
    ORDER_EMAIL_ADDRESS: Option[String],
    ORDER_PHONE_NUMBER: Option[String],
    ORDER_MOBILE_PHONE_NUMBER: Option[String],
    TOTAL_VALUE_ORDER_CURRENCY: Option[String],
    TOTAL_VALUE_ORDER_AMOUNT: Option[String],
    ORIGIN: Option[String],
    WS_DC: Option[String],
    ORDER_UID: Option[String],
    DELIVERY_STREET: Option[String],
    DELIVERY_HOUSE_NUMBER: Option[String],
    DELIVERY_HOUSE_NUMBER_EXT: Option[String],
    DELIVERY_POST_CODE: Option[String],
    DELIVERY_CITY: Option[String],
    DELIVERY_STATE: Option[String],
    DELIVERY_COUNTRY: Option[String],
    DELIVERY_PHONE: Option[String],
    INVOICE_NAME: Option[String],
    INVOICE_STREET: Option[String],
    INVOICE_HOUSE_NUMBER: Option[String],
    INVOICE_HOUSE_NUMBER_EXT: Option[String],
    INVOICE_ZIPCODE: Option[String],
    INVOICE_CITY: Option[String],
    INVOICE_STATE: Option[String],
    INVOICE_COUNTRY: Option[String]
)
