package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOperators

class OperatorStitchUpdateMappingSpec extends SparkJobSpec with TestOperators {

  import spark.implicits._

  val previousIntegratedOpDS1 = Seq(
    defaultOperator.copy(ohubId = Some("1"), concatId = "GB~CATERLYST~3442130"),
    defaultOperator.copy(ohubId = Some("2"), concatId = "IE~CATERLYST~13480540"),
    defaultOperator.copy(ohubId = Some("3"), concatId = "concatId3"),
    defaultOperator.copy(ohubId = Some("4"), concatId = "concatId4")
  ).toDataset

  val operatorsDF1 = Seq(
    defaultOperator.copy(ohubId = Some("13384"), concatId = "GB~CATERLYST~344211"),
    defaultOperator.copy(ohubId = Some("13386"), concatId = "GB~CATERLYST~344212"),
    defaultOperator.copy(ohubId = Some("13385"), concatId = "GB~CATERLYST~344213"),
    defaultOperator.copy(ohubId = Some("13382"), concatId = "IE~CATERLYST~1348054")
  ).toDF()


  val operatorsDF2 = Seq(
    defaultOperator.copy(ohubId = Some("13384"), concatId = "GB~CATERLYST~3442110"),
    defaultOperator.copy(ohubId = Some("13386"), concatId = "GB~CATERLYST~3442120"),
    defaultOperator.copy(ohubId = Some("13385"), concatId = "GB~CATERLYST~3442130"),
    defaultOperator.copy(ohubId = Some("13382"), concatId = "IE~CATERLYST~13480540")
  ).toDF()


  var udlRecord = Seq(
    ("GB~CATERLYST~344213", "GB~CATERLYST~344213"),
    ("IE~CATERLYST~1348054", "IE~CATERLYST~1348054")
  ).toDF("concat_source", "concat_caterlyst")

  describe("OperatorStitchUpdateMapping.transform") {
    it("should map correct id and assign ohub id") {
      val stitchedRecords = OperatorStitchUpdateMapping.transform(spark, operatorsDF1,
        udlRecord, previousIntegratedOpDS1).collect()
      stitchedRecords.length shouldBe(6)
      stitchedRecords.find(_.concatId == "GB~CATERLYST~344213").get.ohubId shouldBe Some("13385")
      stitchedRecords.find(_.concatId == "IE~CATERLYST~1348054").get.ohubId shouldBe Some("13382")

    }

    it("original ohubId when there is no catarlyst concat is match") {
      val stitchedRecords = OperatorStitchUpdateMapping.transform(spark, operatorsDF2,
        udlRecord, previousIntegratedOpDS1).collect()
      print(stitchedRecords)
      stitchedRecords.length shouldBe(4)
      stitchedRecords.find(_.concatId == "GB~CATERLYST~3442130").get.ohubId shouldBe Some("1")
      stitchedRecords.find(_.concatId == "IE~CATERLYST~13480540").get.ohubId shouldBe Some("2")
    }
  }
}
