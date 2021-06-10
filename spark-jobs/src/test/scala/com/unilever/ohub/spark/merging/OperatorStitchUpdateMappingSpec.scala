package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOperators
import org.apache.spark.sql.SparkSession

class OperatorStitchUpdateMappingSpec extends SparkJobSpec with TestOperators {

  import spark.implicits._
  lazy implicit val implicitSpark: SparkSession = spark

  var operators = Seq(
    defaultOperator.copy(ohubId = Some("13384"), concatId = "GB~CATERLYST~782162"),
    defaultOperator.copy(ohubId = Some("13385"), concatId = "GB~CATERLYST~29006300"),
    defaultOperator.copy(ohubId = Some("13382"), concatId = "IE~CATERLYST~1348054")
  ).toDF()

  var deltaPreProcessedOutput = Seq(
    defaultOperator.copy(ohubId = Some("13389"), concatId = "GB~CATERLYST~782162"),
    defaultOperator.copy(ohubId = Some("13381"), concatId = "GB~CATERLYST~29006300"),
    defaultOperator.copy(ohubId = Some("13311"), concatId = "IE~CATERLYST~1348052")
  ).toDataset

  var udlRecord = Seq(
    ("GB~CRM~29006300", "GB~CATERLYST~782162", "2021-01-01"),
    ("GB~CRM~29006300", "GB~CATERLYST~29006300", "2020-01-01")
  ).toDF("concat_source", "concat_caterlyst")
  //var output =


  describe("OperatorStitchUpdateMapping.transform") {
    it("should map correct id and assign ohub id") {

      val stitchedRecords = OperatorStitchUpdateMapping.transform(spark, operators, udlRecord, deltaPreProcessedOutput)
      print(stitchedRecords)
    }
  }

}
