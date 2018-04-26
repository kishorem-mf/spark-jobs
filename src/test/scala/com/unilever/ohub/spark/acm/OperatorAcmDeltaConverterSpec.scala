package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.data.ChannelMapping
import com.unilever.ohub.spark.domain.entity.{ Operator, TestOperators }
import org.apache.spark.sql.Dataset

class OperatorAcmDeltaConverterSpec extends SparkJobSpec with TestOperators {

  private[acm] val SUT = OperatorAcmDeltaConverter

  describe("contact person acm delta converter") {
    it("should convert a domain operator correctly into an acm converter containing only delta records") {
      import spark.implicits._

      val channelMapping = ChannelMapping(countryCode = "country-code", originalChannel = "channel", localChannel = "local-channel", channelUsage = "channel-usage", socialCommercial = "social-commercial", strategicChannel = "strategic-channel", globalChannel = "global-channel", globalSubChannel = "global-sub-channel")
      val channelMappings: Dataset[ChannelMapping] = spark.createDataset(Seq(channelMapping))

      val updatedRecord = defaultOperator.copy(
        isGoldenRecord = true,
        countryCode = "updated",
        concatId = s"updated~${defaultOperator.sourceName}~${defaultOperator.sourceEntityId}",
        city = Some("Utrecht"))

      val deletedRecord = defaultOperator.copy(
        isGoldenRecord = true,
        countryCode = "deleted",
        concatId = s"deleted~${defaultOperator.sourceName}~${defaultOperator.sourceEntityId}",
        isActive = true)

      val newRecord = defaultOperator.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultOperator.sourceName}~${defaultOperator.sourceEntityId}"
      )

      val unchangedRecord = defaultOperator.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultOperator.sourceName}~${defaultOperator.sourceEntityId}"
      )

      val notADeltaRecord = defaultOperator.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultOperator.sourceName}~${defaultOperator.sourceEntityId}"
      )

      val previous: Dataset[Operator] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))
      val input: Dataset[Operator] = spark.createDataset(Seq(
        updatedRecord.copy(city = Some("Amsterdam")),
        deletedRecord.copy(isActive = false),
        unchangedRecord,
        newRecord
      ))

      val result = SUT.transform(spark, channelMappings, input, previous)
        .collect()
        .sortBy(_.OPR_LNKD_INTEGRATION_ID)

      result.length shouldBe 3
      assert(result.head.OPR_LNKD_INTEGRATION_ID == s"deleted~${defaultOperator.sourceName}~${defaultOperator.sourceEntityId}")
      assert(result.head.DELETE_FLAG == "Y")
      assert(result(1).OPR_LNKD_INTEGRATION_ID == s"new~${defaultOperator.sourceName}~${defaultOperator.sourceEntityId}")
      assert(result(2).OPR_LNKD_INTEGRATION_ID == s"updated~${defaultOperator.sourceName}~${defaultOperator.sourceEntityId}")
      assert(result(2).OPR_LNKD_INTEGRATION_ID == s"updated~${defaultOperator.sourceName}~${defaultOperator.sourceEntityId}")
      assert(result(2).CITY.contains("Amsterdam"))
    }
  }
}
