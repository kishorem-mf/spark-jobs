package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.WholesalerAssignment
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class WholesalerAssignmentConverterSpec extends CsvDomainGateKeeperSpec[WholesalerAssignment] {

  override val SUT = WholesalerAssignmentConverter

  describe("common wholesalerassignment converter") {
    it("should convert a wholesalerassignment correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_WHOLESALERASSIGNMENT.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualWS = actualDataSet.head()

        val expectedWS = WholesalerAssignment(
          id = "id-1",
          creationTimestamp = new Timestamp(1542205922011L),
          concatId = "DE~EMAKINA~123",
          countryCode = "DE",
          customerType = "OPERATOR",
          dateCreated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          dateUpdated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          isActive = true,
          isGoldenRecord = false,
          ohubId = Option.empty,
          operatorOhubId = None,
          operatorConcatId = Some("DE~EMAKINA~789"),
          sourceEntityId = "123",
          sourceName = "EMAKINA",
          ohubCreated = actualWS.ohubCreated,
          ohubUpdated = actualWS.ohubUpdated,
          routeToMarketIceCreamCategory = None,
          isPrimaryFoodsWholesaler = Some(true),
          isPrimaryIceCreamWholesaler = Some(true),
          isPrimaryFoodsWholesalerCrm = Some(true),
          isPrimaryIceCreamWholesalerCrm = Some(true),
          wholesalerCustomerCode2 = None,
          wholesalerCustomerCode3 = None,
          hasPermittedToShareSsd = None,
          isProvidedByCrm = None,
          crmId = Some("crm-1"),
          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualWS shouldBe expectedWS
      }
    }
  }
}
