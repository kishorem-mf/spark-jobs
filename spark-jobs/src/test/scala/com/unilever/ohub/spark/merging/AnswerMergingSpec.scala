package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._
import org.apache.spark.sql.Dataset
import com.unilever.ohub.spark.SharedSparkSession.spark

class AnswerMergingSpec extends SparkJobSpec with TestAnswers {

  import spark.implicits._

  private val SUT = AnswerMerging

  describe("Answer merging") {

    it("should give a new answer an ohubId and be marked golden record") {
      val input = Seq(
        defaultAnswer
      ).toDataset

      val previous = Seq[Answer]().toDataset

      val result = SUT.transform(spark, input, previous)
        .collect()

      result.head.ohubId shouldBe defined
      result.head.isGoldenRecord shouldBe true
    }

    it("should take newest data if available while retaining ohubId") {

      val updatedRecord = defaultAnswer.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        concatId = s"updated~${defaultAnswer.sourceName}~${defaultAnswer.sourceEntityId}")

      val deletedRecord = defaultAnswer.copy(
        isGoldenRecord = true,
        countryCode = "deleted",
        concatId = s"deleted~${defaultAnswer.sourceName}~${defaultAnswer.sourceEntityId}",
        isActive = true)

      val newRecord = defaultAnswer.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultAnswer.sourceName}~${defaultAnswer.sourceEntityId}"
      )

      val unchangedRecord = defaultAnswer.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultAnswer.sourceName}~${defaultAnswer.sourceEntityId}"
      )

      val notADeltaRecord = defaultAnswer.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultAnswer.sourceName}~${defaultAnswer.sourceEntityId}"
      )

      val previous: Dataset[Answer] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))

      val input: Dataset[Answer] = spark.createDataset(Seq(
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
