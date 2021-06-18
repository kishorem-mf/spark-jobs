package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.EntityRelationships
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class EntityRelationshipsConverterSpec extends CsvDomainGateKeeperSpec[EntityRelationships] {

  override val SUT = EntityRelationshipsConverter

  describe("common EntityRelationships converter") {
    it("should convert an EntityRelationships correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_ENTITY_RELATIONSHIPS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualER = actualDataSet.head()

        val expectedER = EntityRelationships(
          id = "id-1",
          creationTimestamp = new Timestamp(1542205922011L),
          countryCode = "DE",
          sourceName = "EMAKINA",
          sourceEntityId = "123",
          concatId = "DE~EMAKINA~123",
          relationshipName = Some("relationshipName"),
          relationshipType = "relationshipType",
          fromEntityType = "fromEntityType",
          fromEntitySourceName = "EMAKINA",
          fromSourceEntityId = "123",
          fromConcatId = "DE~EMAKINA~123",
          fromEntityOhubId = Some("555"),
          fromEntityName = Some("Operator"),
          toEntityType = "toEntityType",
          toEntitySourceName = "EMAKINA",
          toSourceEntityId = "456",
          toConcatId = "DE~EMAKINA~456",
          toEntityOhubId = Some("123445"),
          toEntityName = Some("Operator"),
          isActive = true,
          validFrom = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          validTo = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          dateCreated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          dateUpdated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),

          customerType = "OPERATOR",
          ohubCreated = actualER.ohubCreated,
          ohubUpdated = actualER.ohubUpdated,
          ohubId = Option.empty,
          isGoldenRecord = false,

          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualER shouldBe expectedER
      }
    }
  }
}
