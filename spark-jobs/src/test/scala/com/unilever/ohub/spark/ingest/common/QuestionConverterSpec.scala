package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Question
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class QuestionConverterSpec extends CsvDomainGateKeeperSpec[Question] {

  override val SUT = QuestionConverter

  describe("common question converter") {
    it("should convert a question correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_QUESTION.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualQuestion = actualDataSet.head()

        val expectedQuestion = Question(
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
          ohubCreated = actualQuestion.ohubCreated,
          ohubUpdated = actualQuestion.ohubUpdated,

          activityConcatId = "DE~EMAKINA~456",
          question = Some("question"),

          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualQuestion shouldBe expectedQuestion
      }
    }
  }
}
