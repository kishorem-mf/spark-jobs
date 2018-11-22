package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestQuestions
import org.apache.spark.sql.Dataset
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.Question

class QuestionMergingSpec extends SparkJobSpec with TestQuestions {

  import spark.implicits._

  private val SUT = QuestionMerging

  describe("Question merging") {
    it("should give a new question an ohubId and be marked golden record") {
      val input = Seq(
        defaultQuestion
      ).toDataset

      val previous = Seq[Question]().toDataset

      val result = SUT.transform(spark, input, previous)
        .collect()

      result.head.ohubId shouldBe defined
      result.head.isGoldenRecord shouldBe true
    }

    it("should take newest data if available while retaining ohubId") {

      val updatedRecord = defaultQuestion.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        concatId = s"updated~${defaultQuestion.sourceName}~${defaultQuestion.sourceEntityId}")

      val deletedRecord = defaultQuestion.copy(
        isGoldenRecord = true,
        countryCode = "deleted",
        concatId = s"deleted~${defaultQuestion.sourceName}~${defaultQuestion.sourceEntityId}",
        isActive = true)

      val newRecord = defaultQuestion.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultQuestion.sourceName}~${defaultQuestion.sourceEntityId}"
      )

      val unchangedRecord = defaultQuestion.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultQuestion.sourceName}~${defaultQuestion.sourceEntityId}"
      )

      val notADeltaRecord = defaultQuestion.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultQuestion.sourceName}~${defaultQuestion.sourceEntityId}"
      )

      val previous: Dataset[Question] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))
      val input: Dataset[Question] = spark.createDataset(Seq(
        updatedRecord.copy(ohubId = Some("newId")),
        deletedRecord.copy(isActive = false),
        unchangedRecord,
        newRecord
      ))

      val result = SUT.transform(spark, input, previous)
        .collect()
        .sortBy(_.countryCode)

      result.length shouldBe 5
      result(0).isActive shouldBe false

      result(1).countryCode shouldBe "new"

      result(2).countryCode shouldBe "notADelta"

      result(3).countryCode shouldBe "unchanged"

      result(4).countryCode shouldBe "updated"
      result(4).ohubId shouldBe Some("oldId")
    }
  }
}
