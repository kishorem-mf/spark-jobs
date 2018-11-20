package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Answer
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class AnswerConverterSpec extends CsvDomainGateKeeperSpec[Answer] {

  override val SUT = AnswerConverter

  describe("common answer converter") {
    it("should convert an answer correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_ANSWER.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualAnswer = actualDataSet.head()

        val expectedAnswer = Answer(
          id = "id-1",
          creationTimestamp = new Timestamp(1542205922011L),
          concatId = "DE~EMAKINA~123",
          countryCode = "DE",
          customerType = "CONTACTPERSON",
          dateCreated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          dateUpdated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          isActive = true,
          isGoldenRecord = false,
          sourceEntityId = "123",
          sourceName = "EMAKINA",
          ohubId = Option.empty,
          ohubCreated = actualAnswer.ohubCreated,
          ohubUpdated = actualAnswer.ohubUpdated,

          answer = Some("blah"),
          questionConcatId = "DE~EMAKINA~456",

          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualAnswer shouldBe expectedAnswer
      }
    }
  }
}
