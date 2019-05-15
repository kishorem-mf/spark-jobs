package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.export.acm.model.AcmProduct
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object ProductAcmConverter extends Converter[Product, AcmProduct] with TransformationFunctions with AcmTransformationFunctions {

  override def convert(product: Product): AcmProduct = {
    AcmProduct(
      COUNTY_CODE = product.countryCode,
      PRODUCT_NAME = product.name,
      PRD_INTEGRATION_ID = product.ohubId,
      EAN_CODE = product.eanConsumerUnit,
      MRDR_CODE = product.code,
      CREATED_AT = product.ohubCreated,
      UPDATED_AT = product.ohubUpdated,
      DELETE_FLAG = !product.isActive
    )
  }
}
