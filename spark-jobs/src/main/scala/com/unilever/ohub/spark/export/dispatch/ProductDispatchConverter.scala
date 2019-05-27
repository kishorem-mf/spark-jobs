package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.export.dispatch.model.DispatchProduct
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object ProductDispatchConverter extends Converter[Product, DispatchProduct] with TransformationFunctions with DispatchTransformationFunctions {

  override def convert(product: Product): DispatchProduct = {
    DispatchProduct(
      COUNTRY_CODE = product.countryCode,
      SOURCE = product.sourceName,
      CREATED_AT = product.ohubCreated,
      UPDATED_AT = product.ohubUpdated,
      PRODUCT_NAME = product.name,
      EAN_CODE = product.eanConsumerUnit,
      DELETE_FLAG = !product.isActive,
      MRDR_CODE = product.code,
      PRD_INTEGRATION_ID = product.concatId,
      EAN_CODE_DISPATCH_UNIT = product.eanDistributionUnit,
      CATEGORY = product.categoryByMarketeer,
      SUB_CATEGORY = product.subCategoryByMarketeer,
      BRAND = product.brandCode,
      SUB_BRAND = product.subBrandCode,
      ITEM_TYPE = product.`type`,
      UNIT = product.unit,
      UNIT_PRICE_CURRENCY = product.currency,
      UNIT_PRICE = product.unitPrice
    )
  }
}
