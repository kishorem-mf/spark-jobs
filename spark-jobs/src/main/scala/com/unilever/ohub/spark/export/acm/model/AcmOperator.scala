package com.unilever.ohub.spark.export.acm.model

import com.unilever.ohub.spark.export.ACMOutboundEntity

case class AcmOperator(
                        OPR_ORIG_INTEGRATION_ID: String,
                        OPR_LNKD_INTEGRATION_ID: String,
                        GOLDEN_RECORD_FLAG: String,
                        COUNTRY_CODE: String,
                        NAME: String,
                        CHANNEL: String,
                        SUB_CHANNEL: String,
                        ROUTE_TO_MARKET: String = "",
                        REGION: String,
                        OTM: String,
                        PREFERRED_PARTNER: String,
                        STREET: String,
                        HOUSE_NUMBER: String,
                        ZIPCODE: String,
                        CITY: String,
                        COUNTRY: String,
                        AVERAGE_SELLING_PRICE: String,
                        NUMBER_OF_COVERS: String,
                        NUMBER_OF_WEEKS_OPEN: String,
                        NUMBER_OF_DAYS_OPEN: String,
                        CONVENIENCE_LEVEL: String,
                        RESPONSIBLE_EMPLOYEE: String,
                        NPS_POTENTIAL: String,
                        CAM_KEY: String = "",
                        CAM_TEXT: String = "",
                        CHANNEL_KEY: String = "",
                        CHANNEL_TEXT: String = "",
                        CHAIN_KNOTEN: String,
                        CHAIN_NAME: String,
                        CUST_SUB_SEG_EXT: String = "",
                        CUST_SEG_EXT: String = "",
                        CUST_SEG_KEY_EXT: String = "",
                        CUST_GRP_EXT: String = "",
                        PARENT_SEGMENT: String = "",
                        DATE_CREATED: String,
                        DATE_UPDATED: String,
                        DELETE_FLAG: String,
                        WHOLESALER_OPERATOR_ID: String,
                        PRIVATE_HOUSEHOLD: String,
                        VAT: String,
                        OPEN_ON_MONDAY: String,
                        OPEN_ON_TUESDAY: String,
                        OPEN_ON_WEDNESDAY: String,
                        OPEN_ON_THURSDAY: String,
                        OPEN_ON_FRIDAY: String,
                        OPEN_ON_SATURDAY: String,
                        OPEN_ON_SUNDAY: String,
                        KITCHEN_TYPE: String,
                        LOCAL_CHANNEL: String,
                        CHANNEL_USAGE: String,
                        SOCIAL_COMMERCIAL: String,
                        STRATEGIC_CHANNEL: String,
                        GLOBAL_CHANNEL: String,
                        GLOBAL_SUBCHANNEL: String
                        ,SOURCE_IDS: String
                        ,TARGET_OHUB_ID: String
                      ) extends ACMOutboundEntity
