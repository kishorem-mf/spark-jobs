package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOperators

class OperatorCreateGoldenRecordSpecs extends SparkJobSpec with TestOperators {
  import spark.implicits._

  private val SUT = OperatorCreateGoldenRecord;

  describe("Operator create golden record") {
    describe("full transform") {
      // Since calling withColumn for each column in operators is really slow due to the high amount of projects,
      // only 1 full transform is performed. (see https://issues.apache.org/jira/browse/SPARK-7276). On the cluster this
      // is no real deal-breaker a.t.m. Since merging performs fine for a full set of data.

      val opMerge1 = defaultOperator.copy(dateUpdated = Some(new Timestamp(1L)), name = Some("newerOp"), ohubId = Some("tcMerge"))
      val opMerge2 = defaultOperator.copy(dateUpdated = None, name = Some("olderOp"), ohubId = Some("tcMerge"))

      val opNull1 = defaultOperator.copy(dateUpdated = Some(new Timestamp(1L)), name = None, ohubId = Some("tcNull"))
      val opNull2 = defaultOperator.copy(dateUpdated = None, name = Some("olderOp"), ohubId = Some("tcNull"))

      val opInactive = defaultOperator.copy(isActive = false, ohubId = Some("tcInactive"))

      val input = Seq(opMerge1, opMerge2, opNull1, opNull2, opInactive).toDataset

      val result = SUT.transform(spark, input).collect

      it("should output 1 record for each group with active operators") {
        result.length shouldBe(2)
      }

      it("should not output inactive groups") {
        val tcResult = result.filter(_.ohubId == Some("tcInactive"))
        tcResult.length shouldBe 0
      }

      it("should merge 2 records from the same group") {
        val tcResult = result.filter(_.ohubId == Some("tcMerge"))
        tcResult.length shouldBe 1
        tcResult.head.name shouldBe opMerge1.name
      }

      it("should ignore null values when an older value exists within the group") {

      }

      it("should only process active operators") {

      }
    }





  }
}
