package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.Order
import com.unilever.ohub.spark.export.acm.model.AcmOrder
import com.unilever.ohub.spark.export.{Converter, InvertedBooleanToYNConverter, TypeConversionFunctions}

object OrderAcmConverter extends Converter[Order, AcmOrder] with TypeConversionFunctions with AcmTransformationFunctions {

  override def convert(implicit order: Order, explain: Boolean = false): AcmOrder = {
    AcmOrder(
      ORDER_ID = getValue("concatId"),
      REF_ORDER_ID = getValue("sourceEntityId"),
      COUNTRY_CODE = getValue("countryCode"),
      ORDER_TYPE = getValue("type"),
      CP_LNKD_INTEGRATION_ID = getValue("contactPersonOhubId"),
      OPR_LNKD_INTEGRATION_ID = getValue("operatorOhubId"),
      CAMPAIGN_CODE = getValue("campaignCode"),
      CAMPAIGN_NAME = getValue("campaignName"),
      WHOLESALER = getValue("distributorName"),
      WHOLESALER_ID = getValue("distributorId"),
      WHOLESALER_CUSTOMER_NUMBER = getValue("distributorOperatorId"),
      WHOLESALER_LOCATION = getValue("distributorLocation"),
      TRANSACTION_DATE = getValue("transactionDate"),
      ORDER_AMOUNT = getValue("amount"),
      ORDER_AMOUNT_CURRENCY_CODE = getValue("currency"),
      DELIVERY_STREET = getValue("deliveryOperatorStreet"),
      DELIVERY_HOUSENUMBER = getValue("deliveryOperatorHouseNumber"),
      DELIVERY_ZIPCODE = getValue("deliveryOperatorZipCode"),
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
      VAT = getValue("vat"),
      DELETED_FLAG = getValue("isActive", InvertedBooleanToYNConverter)
    )
  }
}
