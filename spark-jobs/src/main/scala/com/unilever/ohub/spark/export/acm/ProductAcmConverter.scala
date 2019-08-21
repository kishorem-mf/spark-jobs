package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.export.acm.model.AcmProduct
import com.unilever.ohub.spark.export.{Converter, InvertedBooleanToYNConverter, TypeConversionFunctions}

object ProductAcmConverter extends Converter[Product, AcmProduct] with TypeConversionFunctions with AcmTypeConversionFunctions {

  override def convert(implicit product: Product, explain: Boolean = false): AcmProduct = {
    AcmProduct(
      COUNTY_CODE = getValue("countryCode"),
      PRODUCT_NAME = getValue("name"),
      PRD_INTEGRATION_ID = getValue("ohubId"),
      EAN_CODE = getValue("eanConsumerUnit"),
      MRDR_CODE = getValue("code"),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated"),
      DELETE_FLAG = getValue("isActive", InvertedBooleanToYNConverter)
    )
  }
}
