package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.Order
import com.unilever.ohub.spark.export.acm.model.AcmOrder
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object OrderAcmConverter extends Converter[Order, AcmOrder] with TransformationFunctions with AcmTransformationFunctions {

  override def convert(order: Order): AcmOrder = {
    AcmOrder(
      ORDER_ID = order.concatId,
      REF_ORDER_ID = order.ohubId,
      COUNTRY_CODE = order.countryCode,
      ORDER_TYPE = order.`type`,
      CP_LNKD_INTEGRATION_ID = order.contactPersonOhubId,
      OPR_LNKD_INTEGRATION_ID = order.operatorConcatId,
      CAMPAIGN_CODE = order.campaignCode,
      CAMPAIGN_NAME = order.campaignName,
      WHOLESALER = order.distributorId,
      TRANSACTION_DATE = order.transactionDate,
      ORDER_AMOUNT = order.amount,
      ORDER_AMOUNT_CURRENCY_CODE = order.currency,
      DELIVERY_STREET = order.deliveryOperatorStreet,
      DELIVERY_HOUSENUMBER = order.deliveryOperatorHouseNumber,
      DELIVERY_ZIPCODE = order.deliveryOperatorZipCode,
      DELIVERY_CITY = order.deliveryOperatorCity,
      DELIVERY_STATE = order.deliveryOperatorState,
      DELIVERY_COUNTRY = order.deliveryOperatorCountry,
      INVOICE_NAME = order.invoiceOperatorName,
      INVOICE_STREET = order.invoiceOperatorStreet,
      INVOICE_HOUSE_NUMBER = order.invoiceOperatorHouseNumber,
      INVOICE_HOUSE_NUMBER_EXT = order.invoiceOperatorHouseNumberExtension,
      INVOICE_ZIPCODE = order.invoiceOperatorZipCode,
      INVOICE_CITY = order.invoiceOperatorCity,
      INVOICE_STATE = order.invoiceOperatorState,
      INVOICE_COUNTRY = order.invoiceOperatorCountry,
      COMMENTS = order.comment,
      VAT = order.vat,
      DELETED_FLAG = !order.isActive
    )
  }
}
