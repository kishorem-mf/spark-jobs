package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{EntityRelationships, TestEntityRelationships}
import org.apache.spark.sql.Dataset

class EntityRelationshipsMergingSpec extends SparkJobSpec with TestEntityRelationships {

  import spark.implicits._

  private val SUT = EntityRelationshipsMerging

  describe("ER merging") {

    it("should give a new ER an ohubId and be marked golden record") {
      val input = Seq(
        defaultEntityRelationships
      ).toDataset

      val previous = Seq[EntityRelationships]().toDataset

      val result = SUT.transform(spark, input, previous)
        .collect()

      result.head.ohubId shouldBe defined
      result.head.isGoldenRecord shouldBe true
    }

    it("should take newest data if available while retaining ohubId") {

      val updatedRecord = defaultEntityRelationships.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        concatId = s"updated~${defaultEntityRelationships.sourceName}~${defaultEntityRelationships.sourceEntityId}")

      val deletedRecord = defaultEntityRelationships.copy(
        isGoldenRecord = true,
        countryCode = "deleted",
        concatId = s"deleted~${defaultEntityRelationships.sourceName}~${defaultEntityRelationships.sourceEntityId}",
        isActive = true)

      val newRecord = defaultEntityRelationships.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultEntityRelationships.sourceName}~${defaultEntityRelationships.sourceEntityId}"
      )

      val unchangedRecord = defaultEntityRelationships.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultEntityRelationships.sourceName}~${defaultEntityRelationships.sourceEntityId}"
      )

      val notADeltaRecord = defaultEntityRelationships.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultEntityRelationships.sourceName}~${defaultEntityRelationships.sourceEntityId}"
      )

      val previous: Dataset[EntityRelationships] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))

      val input: Dataset[EntityRelationships] = spark.createDataset(Seq(
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
