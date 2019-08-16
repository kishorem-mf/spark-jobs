package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.Order
import com.unilever.ohub.spark.export.dispatch.model.DispatchOrder
import com.unilever.ohub.spark.export.{Converter, InvertedBooleanToYNConverter, ToUpperCase, TypeConversionFunctions}

object OrderDispatchConverter extends Converter[Order, DispatchOrder] with TypeConversionFunctions with DispatchTransformationFunctions {

  override def convert(implicit order: Order, explain: Boolean = false): DispatchOrder = {
    DispatchOrder(
      SOURCE_ID = getValue("sourceEntityId"),
      ORD_INTEGRATION_ID = getValue("concatId"),
      COUNTRY_CODE = getValue("countryCode"),
      SOURCE = getValue("sourceName"),
      DELETED_FLAG = getValue("isActive", InvertedBooleanToYNConverter),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated"),
      ORDER_TYPE = getValue("type", ToUpperCase),
      CP_ORIG_INTEGRATION_ID = getValue("contactPersonConcatId"),
      OPR_ORIG_INTEGRATION_ID = getValue("operatorConcatId"),
      WHOLESALER = getValue("distributorName"),
      WHOLESALER_ID = getValue("distributorId"),
      WHOLESALER_CUSTOMER_NUMBER = getValue("distributorOperatorId"),
      WHOLESALER_LOCATION = getValue("distributorLocation"),
      TRANSACTION_DATE = getValue("transactionDate"),
      ORDER_UID = getValue("orderUid"),
      CAMPAIGN_CODE = getValue("campaignCode"),
      CAMPAIGN_NAME = getValue("campaignName"),
      DELIVERY_STREET = getValue("deliveryOperatorStreet"),
      DELIVERY_HOUSE_NUMBER = getValue("deliveryOperatorHouseNumber"),
      DELIVERY_HOUSE_NUMBER_EXT = getValue("deliveryOperatorHouseNumberExtension"),
      DELIVERY_POST_CODE = getValue("deliveryOperatorZipCode"),
      DELIVERY_CITY = getValue("deliveryOperatorCity"),
      DELIVERY_STATE = getValue("deliveryOperatorState"),
      DELIVERY_COUNTRY = getValue("deliveryOperatorCountry"),
      INVOICE_NAME = getValue("invoiceOperatorName"),
      INVOICE_STREET = getValue("invoiceOperatorStreet"),
      INVOICE_HOUSE_NUMBER = getValue("invoiceOperatorHouseNumber"),
      INVOICE_HOUSE_NUMBER_EXT = getValue("invoiceOperatorHouseNumberExtension"),
      INVOICE_ZIPCODE = getValue("invoiceOperatorZipCode"),
      INVOICE_CITY = getValue("invoiceOperatorCity"),
      INVOICE_STATE = getValue("invoiceOperatorState"),
      INVOICE_COUNTRY = getValue("invoiceOperatorCountry"),
      COMMENTS = getValue("comment"),
      VAT = getValue("vat")
    )
  }
}
