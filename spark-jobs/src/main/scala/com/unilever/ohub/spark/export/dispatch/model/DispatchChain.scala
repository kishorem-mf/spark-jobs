package com.unilever.ohub.spark.export.dispatch.model

import com.unilever.ohub.spark.export.OutboundEntity

case class DispatchChain(
                          CHAIN_INTEGRATION_ID: String,
                          CUSTOMER_TYPE: String,
                          SOURCE: String,
                          COUNTRY_CODE: String,
                          CREATED_AT: String,
                          UPDATED_AT: String,
                          DELETE_FLAG: String,
                          CONCEPT_NAME: String = "",
                          NUM_OF_UNITS: String = "",
                          NUM_OF_STATES: String = "",
                          ESTIMATED_ANNUAL_SALES: String = "",
                          ESTIMATED_PURCHASE_POTENTIAL: String = "",
                          ADDRESS: String = "",
                          CITY: String = "",
                          STATE: String = "",
                          ZIP_CODE: String = "",
                          WEBSITE: String = "",
                          PHONE_NUMBER: String = "",
                          SEGMENT: String = "",
                          PRIMARY_MENU: String = "",
                          SECONDARY_MENU: String = ""
                        ) extends OutboundEntity
