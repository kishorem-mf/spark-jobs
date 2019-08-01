package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.{Chain}
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class ChainConverterSpec extends CsvDomainGateKeeperSpec[Chain] {

  override val SUT = ChainConverter

  describe("common chain converter") {
    it("should convert a chain correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_CHAINS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualChain = actualDataSet.head()

        val expectedChain = Chain(
          id = "1",
          creationTimestamp = new Timestamp(1563806646631L),
          concatId = "US~FIREFLY~10002662",
          countryCode = "US",
          customerType = Chain.customerType,
          ohubCreated = actualChain.ohubCreated,
          ohubUpdated = actualChain.ohubCreated,
          ohubId = Option.empty,
          isGoldenRecord = false,
          sourceEntityId = "10002662",
          sourceName = "FIREFLY",
          isActive = true,
          dateCreated = Some(Timestamp.valueOf("2018-10-08 22:53:51")),
          dateUpdated = Some(Timestamp.valueOf("2018-10-08 22:53:52")),
          conceptName = Some("SWEETFIN POKE"),
          numberOfUnits = Some(9),
          numberOfStates = Some(1),
          estimatedAnnualSales = Some(BigDecimal("15168900.000000000000000000")),
          estimatedPurchasePotential = Some(BigDecimal("4550670.000000000000000000")),
          address = Some("1st Lane"),
          city = Some("New York"),
          state = Some("New York"),
          zipCode = Some("12112AA"),
          website = Some("www.sweetfinpoke.com"),
          phone = Some("1234"),
          segment = Some("Fast Casual"),
          primaryMenu = Some("OTHER ASIAN"),
          secondaryMenu = Some("OTHER ASIAN"),
          additionalFields = Map(),
          ingestionErrors = Map())

        actualChain shouldBe expectedChain
      }
    }
  }
}
