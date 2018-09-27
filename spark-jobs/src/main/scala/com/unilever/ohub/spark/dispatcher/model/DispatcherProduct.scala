package com.unilever.ohub.spark.dispatcher
package model

object DispatcherProduct {
  def fromProduct(product: com.unilever.ohub.spark.domain.entity.Product): DispatcherProduct = {
    DispatcherProduct(
      BRAND = product.brandName,
      PRD_INTEGRATION_ID = product.concatId,
      COUNTRY_CODE = product.countryCode,
      UNIT_PRICE_CURRENCY = product.currency,
      EAN_CODE = product.eanConsumerUnit,
      EAN_CODE_DISPATCH_UNIT = product.eanDistributionUnit,
      DELETE_FLAG = product.isActive.invert,
      PRODUCT_NAME = product.name,
      CREATED_AT = product.ohubCreated.mapWithDefaultPattern,
      UPDATED_AT = product.ohubUpdated.mapWithDefaultPattern,
      MRDR_CODE = product.packagingCode,
      SOURCE = product.sourceName,
      SUB_BRAND = product.subBrandName,
      SUB_CATEGORY = product.subCategoryCode,
      ITEM_TYPE = product.`type`,
      UNIT = product.unit,
      UNIT_PRICE = product.unitPrice.formatTwoDecimalsOpt,
      CATEGORY = None
    )
  }
}

case class DispatcherProduct(
    BRAND: Option[String],
    PRD_INTEGRATION_ID: String,
    COUNTRY_CODE: String,
    UNIT_PRICE_CURRENCY: Option[String],
    EAN_CODE: Option[String],
    EAN_CODE_DISPATCH_UNIT: Option[String],
    DELETE_FLAG: Boolean,
    PRODUCT_NAME: String,
    CREATED_AT: String,
    UPDATED_AT: String,
    MRDR_CODE: Option[String],
    SOURCE: String,
    SUB_BRAND: Option[String],
    SUB_CATEGORY: Option[String],
    ITEM_TYPE: Option[String],
    UNIT: Option[String],
    UNIT_PRICE: Option[String],
    CATEGORY: Option[String]
)
