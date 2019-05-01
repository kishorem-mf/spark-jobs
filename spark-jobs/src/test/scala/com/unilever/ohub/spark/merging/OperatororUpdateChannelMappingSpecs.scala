package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._

class OperatororUpdateChannelMappingSpecs extends SparkJobSpec with TestOperators with TestChannelMappings {

  import spark.implicits._

  private val SUT = OperatorUpdateChannelMapping

  describe("Operator Update Channel Mapping") {
    it("should update operator with fully available channel mapping") {
      val operator = defaultOperator.copy(countryCode = "GB", channel = Some("Cafe or something"))
      val channelMapping = defaultChannelMapping.copy(countryCode = "GB", originalChannel = "Cafe or something", channelReference = "1")
      val channelReference = defaultChannelReference.copy(channelReferenceId = "1")

      val operatorDataset = Seq(operator).toDataset
      val channelMappingDataset = Seq(channelMapping).toDataset
      val channelReferenceDataset = Map(channelReference.channelReferenceId -> channelReference)

      val res = SUT.transform(spark, operatorDataset, channelMappingDataset, channelReferenceDataset).collect

      res.length shouldBe 1
      res.head.localChannel shouldBe Some(channelMapping.localChannel)
      res.head.channelUsage shouldBe Some(channelMapping.channelUsage)
      res.head.socialCommercial shouldBe channelReference.socialCommercial
      res.head.strategicChannel shouldBe Some(channelReference.strategicChannel)
      res.head.globalChannel shouldBe Some(channelReference.globalChannel)
      res.head.globalSubchannel shouldBe Some(channelReference.globalSubchannel)
    }

    it("should update operator with available channel mapping and unknown channelreference (id = -1)") {
      val operator = defaultOperator.copy(countryCode = "NL", channel = Some("This is restaurant"))
      val channelMapping = defaultChannelMapping.copy(countryCode = "NL", originalChannel = "This is restaurant", channelReference = "non-existent")
      val channelReference = defaultChannelReference.copy(channelReferenceId = "-1")

      val operatorDataset = Seq(operator).toDataset
      val channelMappingDataset = Seq(channelMapping).toDataset
      val channelReferenceDataset = Map(channelReference.channelReferenceId -> channelReference)

      val res = SUT.transform(spark, operatorDataset, channelMappingDataset, channelReferenceDataset).collect

      res.length shouldBe 1
      res.head.localChannel shouldBe Some(channelMapping.localChannel)
      res.head.channelUsage shouldBe Some(channelMapping.channelUsage)
      res.head.socialCommercial shouldBe channelReference.socialCommercial
      res.head.strategicChannel shouldBe Some(channelReference.strategicChannel)
      res.head.globalChannel shouldBe Some(channelReference.globalChannel)
      res.head.globalSubchannel shouldBe Some(channelReference.globalSubchannel)
    }

    it("shouldn't update operator when channelMapping is non-existent") {
      val operator = defaultOperator.copy(countryCode = "BE", channel = Some("This is restaurant"))
      val channelMapping = defaultChannelMapping.copy(countryCode = "NOT-BE", originalChannel = "This is restaurant", channelReference = "non-existent")
      val channelReference = defaultChannelReference.copy(channelReferenceId = "-1")

      val operatorDataset = Seq(operator).toDataset
      val channelMappingDataset = Seq(channelMapping).toDataset
      val channelReferenceDataset = Map(channelReference.channelReferenceId -> channelReference)

      val res = SUT.transform(spark, operatorDataset, channelMappingDataset, channelReferenceDataset).collect

      res.length shouldBe 1
      res.head.localChannel shouldBe None
      res.head.channelUsage shouldBe None
      res.head.socialCommercial shouldBe None
      res.head.strategicChannel shouldBe None
      res.head.globalChannel shouldBe None
      res.head.globalSubchannel shouldBe None
    }
  }
}
