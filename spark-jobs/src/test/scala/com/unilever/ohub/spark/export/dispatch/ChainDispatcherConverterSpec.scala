package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestChains
import com.unilever.ohub.spark.export.dispatch.model.DispatchChain

class ChainDispatcherConverterSpec extends SparkJobSpec with TestChains {

  val SUT = ChainDispatchConverter

  describe("Chain dispatch converter") {
    it("should convert chain to dispatchChain") {

      val result = SUT.convert(defaultChains)

      val expectedChains = DispatchChain(
        CHAIN_INTEGRATION_ID = "DE~FAIRKITCHENS~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        CUSTOMER_TYPE = "CHAIN",
        SOURCE = "FAIRKITCHENS",
        COUNTRY_CODE = "DE",
        CREATED_AT = "2019-07-30 13:49:00",
        UPDATED_AT = "2019-07-30 13:49:00",
        DELETE_FLAG = "N",
        CONCEPT_NAME =  "newPromo",
        NUM_OF_UNITS = "23",
        NUM_OF_STATES = "233",
        ESTIMATED_ANNUAL_SALES = "33.23",
        ESTIMATED_PURCHASE_POTENTIAL = "23.93",
        ADDRESS = "33 weena",
        CITY = "Rotterdam",
        STATE = "",
        ZIP_CODE = "",
        WEBSITE = "www.google.com",
        PHONE_NUMBER = "",
        SEGMENT = "",
        PRIMARY_MENU = "sause",
        SECONDARY_MENU = ""
      )

      result shouldBe expectedChains
    }
  }
}
