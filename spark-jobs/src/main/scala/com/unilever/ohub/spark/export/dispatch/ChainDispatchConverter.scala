package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.Chain
import com.unilever.ohub.spark.export.dispatch.model.DispatchChain
import com.unilever.ohub.spark.export.{Converter, InvertedBooleanToYNConverter}

object ChainDispatchConverter extends Converter[Chain, DispatchChain] with DispatchTypeConversionFunctions {

  override def convert(implicit chain: Chain, explain: Boolean = false): DispatchChain = {
    DispatchChain(
      CHAIN_INTEGRATION_ID = getValue("concatId"),
      SOURCE = getValue("sourceName"),
      COUNTRY_CODE = getValue("countryCode"),
      CUSTOMER_TYPE = getValue("customerType"),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated"),
      DELETE_FLAG = getValue("isActive", InvertedBooleanToYNConverter),

      CONCEPT_NAME = getValue("conceptName"),
      NUM_OF_UNITS = getValue("numberOfUnits"),
      NUM_OF_STATES = getValue("numberOfStates"),
      ESTIMATED_ANNUAL_SALES = getValue("estimatedAnnualSales"),
      ESTIMATED_PURCHASE_POTENTIAL = getValue("estimatedPurchasePotential"),
      ADDRESS = getValue("address"),
      CITY = getValue("city"),
      STATE = getValue("state"),
      ZIP_CODE = getValue("zipCode"),
      WEBSITE = getValue("website"),
      PHONE_NUMBER = getValue("phone"),
      SEGMENT = getValue("segment"),
      PRIMARY_MENU = getValue("primaryMenu"),
      SECONDARY_MENU = getValue("secondaryMenu")
    )
  }
}
