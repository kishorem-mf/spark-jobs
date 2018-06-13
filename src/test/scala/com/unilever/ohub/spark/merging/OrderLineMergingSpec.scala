package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOrderLines
import org.apache.spark.sql.Dataset
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.OrderLine

class OrderLineMergingSpec extends SparkJobSpec with TestOrderLines {

  import spark.implicits._

  private val SUT = OrderLineMerging

  describe("product merging") {
    it("should take newest data if available while retaining ohubId") {
      val updatedRecord = defaultOrderLine.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        concatId = s"updated~${defaultOrderLine.sourceName}~${defaultOrderLine.sourceEntityId}",
        comment = Some("Calve"))

      val deletedRecord = defaultOrderLine.copy(
        isGoldenRecord = true,
        countryCode = "deleted",
        concatId = s"deleted~${defaultOrderLine.sourceName}~${defaultOrderLine.sourceEntityId}",
        isActive = true)

      val newRecord = defaultOrderLine.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultOrderLine.sourceName}~${defaultOrderLine.sourceEntityId}"
      )

      val unchangedRecord = defaultOrderLine.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultOrderLine.sourceName}~${defaultOrderLine.sourceEntityId}"
      )

      val notADeltaRecord = defaultOrderLine.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultOrderLine.sourceName}~${defaultOrderLine.sourceEntityId}"
      )

      val previous: Dataset[OrderLine] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))
      val input: Dataset[OrderLine] = spark.createDataset(Seq(
        updatedRecord.copy(comment = Some("Unox"), ohubId = Some("newId")),
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
      result(4).comment shouldBe Some("Unox")
      result(4).ohubId shouldBe Some("oldId")
    }
  }
}
