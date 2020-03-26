package com.unilever.ohub.spark.export.acm.model

import com.unilever.ohub.spark.export.ACMOutboundEntity

case class AcmProduct(
                       COUNTY_CODE: String,
                       PRODUCT_NAME: String,
                       PRD_INTEGRATION_ID: String,
                       EAN_CODE: String,
                       MRDR_CODE: String,
                       CREATED_AT: String,
                       UPDATED_AT: String,
                       DELETE_FLAG: String,
                       TYPE: String) extends ACMOutboundEntity
