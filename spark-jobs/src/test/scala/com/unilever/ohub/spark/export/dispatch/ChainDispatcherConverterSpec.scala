package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestChains
import com.unilever.ohub.spark.export.dispatch.model.DispatchChain

class ChainDispatcherConverterSpec extends SparkJobSpec with TestChains {

  val SUT = ChainDispatchConverter

  describe("Chain dispatch converter") {
    it("should convert chain to dispatchChain") {

      val result = SUT.convert(defaultChain)

      val expectedChains = DispatchChain(
        CHAIN_INTEGRATION_ID = "US~FIREFLY~10002662",
        CUSTOMER_TYPE = "CHAIN",
        SOURCE = "FIREFLY",
        COUNTRY_CODE = "US",
        CREATED_AT = "2015-06-30 13:49:00",
        UPDATED_AT = "2015-06-30 13:49:00",
        DELETE_FLAG = "N",
        CONCEPT_NAME =  "SWEETFIN POKE",
        NUM_OF_UNITS = "9",
        NUM_OF_STATES = "1",
        ESTIMATED_ANNUAL_SALES = "15168900.12",
        ESTIMATED_PURCHASE_POTENTIAL = "4550670.14",
        ADDRESS = "1st Lane",
        CITY = "New York",
        STATE = "New York",
        ZIP_CODE = "12112AA",
        WEBSITE = "www.sweetfinpoke.com",
        PHONE_NUMBER = "1234",
        SEGMENT = "Fast Casual",
        PRIMARY_MENU = "OTHER ASIAN",
        SECONDARY_MENU = "OTHER ASIAN"
      )

      result shouldBe expectedChains
    }
  }
}
