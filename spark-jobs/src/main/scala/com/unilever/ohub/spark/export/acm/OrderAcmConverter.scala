package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.Order
import com.unilever.ohub.spark.export.acm.model.AcmOrder
import com.unilever.ohub.spark.export.{Converter, TypeConversionFunctions}

object OrderAcmConverter extends Converter[Order, AcmOrder] with TypeConversionFunctions with AcmTransformationFunctions {

  override def convert(order: Order): AcmOrder = {
    AcmOrder(
      ORDER_ID = order.concatId,
      REF_ORDER_ID = order.sourceEntityId,
      COUNTRY_CODE = order.countryCode,
      ORDER_TYPE = order.`type`,
      CP_LNKD_INTEGRATION_ID = order.contactPersonOhubId,
      OPR_LNKD_INTEGRATION_ID = order.operatorOhubId,
      CAMPAIGN_CODE = order.campaignCode,
      CAMPAIGN_NAME = order.campaignName,
      WHOLESALER = order.distributorName,
      WHOLESALER_ID = order.distributorId,
      WHOLESALER_CUSTOMER_NUMBER = order.distributorOperatorId,
      WHOLESALER_LOCATION = order.distributorLocation,
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
      DELETED_FLAG = booleanToYNConverter(!order.isActive)
    )
  }
}
