package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.export.dispatch.model.DispatchProduct
import com.unilever.ohub.spark.export.{Converter, InvertedBooleanToYNConverter, TypeConversionFunctions}

object ProductDispatchConverter extends Converter[Product, DispatchProduct] with TypeConversionFunctions with DispatchTypeConversionFunctions {

  override def convert(implicit product: Product, explain: Boolean = false): DispatchProduct = {
    DispatchProduct(
      COUNTRY_CODE = getValue("countryCode"),
      SOURCE = getValue("sourceName"),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated"),
      PRODUCT_NAME = getValue("name"),
      EAN_CODE = getValue("eanConsumerUnit"),
      DELETE_FLAG = getValue("isActive", InvertedBooleanToYNConverter),
      MRDR_CODE = getValue("code"),
      PRD_INTEGRATION_ID = getValue("concatId"),
      EAN_CODE_DISPATCH_UNIT = getValue("eanDistributionUnit"),
      CATEGORY = getValue("categoryByMarketeer"),
      SUB_CATEGORY = getValue("subCategoryByMarketeer"),
      BRAND = getValue("brandCode"),
      SUB_BRAND = getValue("subBrandCode"),
      ITEM_TYPE = getValue("type"),
      UNIT = getValue("unit"),
      UNIT_PRICE_CURRENCY = getValue("currency"),
      UNIT_PRICE = getValue("unitPrice")
    )
  }
}
