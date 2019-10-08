package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOperatorsGolden

class OperatorCreatePerfectGoldenRecordSpecs extends SparkJobSpec with TestOperatorsGolden {
  import spark.implicits._

  private val SUT = OperatorCreatePerfectGoldenRecord;

  describe("Operator create golden record") {
    describe("full transform") {
      // Since calling withColumn for each column in operators is really slow due to the high amount of projects,
      // only 1 full transform is performed. (see https://issues.apache.org/jira/browse/SPARK-7276). On the cluster this
      // is no real deal-breaker a.t.m. Since merging performs fine for a full set of data.

      val opMerge1 = defaultOperatorGolden.copy(
        dateUpdated = Some(new Timestamp(1L)),
        name = Some("olderOp"),
        ohubId = Some("tcMerge"),
        dateCreated = Some(Timestamp.valueOf("2017-10-16 18:09:49")),
        sourceName = "EMAKINA"
      )

      val opMerge2 = defaultOperatorGolden.copy(
        dateUpdated = None,
        name = Some("anotherOldOp"),
        ohubId = Some("tcMerge"),
        sourceName = "FUZZIT"
      )

      val opMerge3 = defaultOperatorGolden.copy(
        dateUpdated = None,
        name = Some("newerOp"),
        ohubId = Some("tcMerge"),
        dateCreated = Some(Timestamp.valueOf("2017-10-17 18:09:49")),
        sourceName = "FUZZIT"
      )

      val opNoMerging = defaultOperatorGolden.copy(
        dateUpdated = None,
        name = Some("olderOp1"),
        ohubId = Some("tcNoMerging"),
        sourceName = "EMAKINA"
      )

      val opNull1 = defaultOperatorGolden.copy(
        dateUpdated = Some(new Timestamp(1L)),
        name = None,
        ohubId = Some("tcNull")
      )

      val opNull2 = defaultOperatorGolden.copy(
        dateUpdated = None,
        name = Some("olderOp"),
        ohubId = Some("tcNull")
      )

      val opInactive = defaultOperatorGolden.copy(isActive = false, ohubId = Some("tcInactive"))

      val opNewest1 = defaultOperatorGolden.copy(
        dateUpdated = Some(new Timestamp(1L)),
        dateCreated = Some(new Timestamp(1L)),
        ohubUpdated = new Timestamp(1L),
        name = None,
        chainName = None,
        channel = Some("newest"),
        ohubId = Some("tcNewest")
      )

      val opNewest2 = defaultOperatorGolden.copy(
        dateUpdated = None,
        dateCreated = Some(new Timestamp(1L)),
        ohubUpdated = new Timestamp(1L),
        name = None,
        chainName = Some("middle"),
        channel = Some("middle"),
        ohubId = Some("tcNewest")
      )

      val opNewest3 = defaultOperatorGolden.copy(
        dateUpdated = None,
        dateCreated = None,
        ohubUpdated = new Timestamp(1L),
        chainName = Some("oldest"),
        channel = Some("oldest"),
        ohubId = Some("tcNewest")
      )

      val opSameDateUpdated1 = defaultOperatorGolden.copy(
        dateUpdated = Some(new Timestamp(1561413600000L)), // 06/25/2019
        dateCreated = Some(new Timestamp(1560981600000L)), // 06/20/2019
        ohubUpdated = new Timestamp(1561845600000L), // 06/30/2019
        name = None,
        chainName = None,
        channel = Some("newest"),
        ohubId = Some("tcSameDateUpdated")
      )

      val opSameDateUpdated2 = defaultOperatorGolden.copy(
        dateUpdated = Some(new Timestamp(1561413600000L)), // 06/25/2019
        dateCreated = Some(new Timestamp(1560204000000L)), // 06/11/2019
        ohubUpdated = new Timestamp(1561845600000L), // 06/30/2019
        name = None,
        chainName = Some("middle"),
        channel = Some("middle"),
        ohubId = Some("tcSameDateUpdated")
      )

      val opSameDateUpdated3 = defaultOperatorGolden.copy(
        dateUpdated = Some(new Timestamp(1561413600000L)), // 06/25/2019
        dateCreated = Some(new Timestamp(1559340000000L)), // 06/01/2019
        ohubUpdated = new Timestamp(1561845600000L), // 06/30/2019
        name = Some("oldest"),
        chainName = Some("oldest"),
        channel = Some("oldest"),
        ohubId = Some("tcSameDateUpdated")
      )

      val input = Seq(opMerge1, opMerge2, opMerge3, opNull1, opNull2, opInactive, opNewest1, opNewest2, opNewest3,
        opSameDateUpdated1, opSameDateUpdated2, opSameDateUpdated3, opNoMerging
      ).toDataset

      val result = SUT.transform(spark, input).collect

      it("should output 1 record for each group with active operators") {
        result.length shouldBe (5)
      }

      it("should not output inactive groups") {
        val tcResult = result.filter(_.ohubId == Some("tcInactive"))
        tcResult.length shouldBe 0
      }

      it("should merge 2 records from the same group") {
        val tcResult = result.filter(_.ohubId == Some("tcMerge"))
        tcResult.length shouldBe 1
        tcResult.head.name shouldBe opMerge3.name
        tcResult.head.sourceName shouldBe "EMAKINA,FUZZIT"
      }

      it("should not merge sources when there is only 1 group") {
        val tcResult = result.filter(_.ohubId == Some("tcNoMerging"))
        tcResult.length shouldBe 1
        tcResult.head.sourceName shouldBe "EMAKINA"
      }

      it("should merge groups based on multiple date columns") {
        val tcResult = result.filter(_.ohubId == Some("tcNewest"))
        tcResult.length shouldBe 1
        tcResult.head.name shouldBe opNewest3.name
        tcResult.head.chainName shouldBe opNewest2.chainName
        tcResult.head.channel shouldBe opNewest1.channel
      }

      it("should merge and use dateCreated if dateupdated is alsways the same") {
        val tcResult = result.filter(_.ohubId == Some("tcSameDateUpdated"))
        tcResult.length shouldBe 1
        tcResult.head.dateUpdated shouldBe opSameDateUpdated1.dateUpdated
        tcResult.head.name shouldBe opSameDateUpdated3.name
        tcResult.head.chainName shouldBe opSameDateUpdated2.chainName
        tcResult.head.channel shouldBe opSameDateUpdated1.channel
        tcResult.head.dateCreated shouldBe opSameDateUpdated3.dateCreated
      }
    }
  }
}
