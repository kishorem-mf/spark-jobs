package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.Order
import com.unilever.ohub.spark.export.dispatch.model.DispatchOrder
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object OrderDispatchConverter extends Converter[Order, DispatchOrder] with TransformationFunctions with DispatchTransformationFunctions {

  override def convert(order: Order): DispatchOrder = {
    DispatchOrder(
      SOURCE_ID = order.sourceEntityId,
      ORD_INTEGRATION_ID = order.concatId,
      COUNTRY_CODE = order.countryCode,
      SOURCE = order.sourceName,
      DELETED_FLAG = booleanToYNConverter(!order.isActive),
      CREATED_AT = order.ohubCreated,
      UPDATED_AT = order.ohubUpdated,
      ORDER_TYPE = order.`type`.toUpperCase,
      CP_ORIG_INTEGRATION_ID = order.contactPersonConcatId,
      OPR_ORIG_INTEGRATION_ID = order.operatorConcatId,
      WHOLESALER = order.distributorName,
      WHOLESALER_ID = order.distributorId,
      WHOLESALER_CUSTOMER_NUMBER = order.distributorOperatorId,
      WHOLESALER_LOCATION = order.distributorLocation,
      TRANSACTION_DATE = order.transactionDate,
      ORDER_UID = order.orderUid,
      CAMPAIGN_CODE = order.campaignCode,
      CAMPAIGN_NAME = order.campaignName,
      DELIVERY_STREET = order.deliveryOperatorStreet,
      DELIVERY_HOUSE_NUMBER = order.deliveryOperatorHouseNumber,
      DELIVERY_HOUSE_NUMBER_EXT = order.deliveryOperatorHouseNumberExtension,
      DELIVERY_POST_CODE = order.deliveryOperatorZipCode,
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
      VAT = order.vat
    )
  }
}
