package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.merging.DataFrameHelpers._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class DummyRecord(id: Int, group: String, ohubId: String)

case class LatestRecord(id: Int, concatId: String, inDelta: Boolean)

case class SingleValue(value: String)

class GroupingFunctionsSpec extends SparkJobSpec {

  import spark.implicits._

  lazy implicit val implicitSpark: SparkSession = spark

  describe("DataFrameHelpers.concatenateColumns") {
    it("should concatenate given columns") {
      val df = Seq(
        DummyRecord(1, "foo", null),
        DummyRecord(2, null, "bar"),
        DummyRecord(3, "foo", "bar"))
        .toDS

      val res = df
        .concatenateColumns("value", Seq("ohubId", "group").map(col))
        .as[SingleValue]
        .collect()

      res.head shouldBe SingleValue("foo")
      res(1) shouldBe SingleValue("bar")
      res(2) shouldBe SingleValue("barfoo")
    }
  }
  describe("DataFrameHelpers.columnsNotNullAndNotEmpty") {
    it("should filter out all records where columns are null of empty") {
      val df = Seq(
        DummyRecord(1, "Dave", "Mustaine"),
        DummyRecord(2, null, "Mustaine"),
        DummyRecord(3, "Dave", null),
        DummyRecord(4, null, null),
        DummyRecord(5, "", "Mustaine"),
        DummyRecord(6, "Dave", "")
      ).toDS

      val res = df
        .columnsNotNullAndNotEmpty($"group", $"ohubId")
        .as[DummyRecord]
        .collect

      res.length shouldBe 1
      res.head.id shouldBe 1
    }
  }

  describe("DataFrameHelpers.addOhubId") {
    it("should select existing ohubId and add one if none exists for the window") {
      val df = Seq(
        DummyRecord(1, "group1", null),
        DummyRecord(2, "group1", "id1"),
        DummyRecord(3, "group2", null),
        DummyRecord(4, "group2", null)
      ).toDS

      val res = df.addOhubId
        .as[DummyRecord]
        .sort($"id".asc)
        .collect()

      res.head.ohubId shouldBe "id1"
      res(1).ohubId shouldBe "id1"
      res(2).ohubId.length shouldBe 36
      res(3).ohubId.length shouldBe 36
      res(2).ohubId shouldBe res(3).ohubId
    }
  }

  describe("DataFrameHelpers.selectLatestRecord") {
    it("should select latest (delta) record if there is one") {

      val df = Seq(
        LatestRecord(1, "id1", inDelta = false),
        LatestRecord(2, "id1", inDelta = true),
        LatestRecord(3, "id2", inDelta = false),
        LatestRecord(4, "id3", inDelta = true)
      ).toDS

      val res = df
        .selectLatestRecord
        .as[LatestRecord]
        .sort($"id".asc)
        .collect

      res.length shouldBe 3
      res.head.id shouldBe 2
      res(1).id shouldBe 3
      res(2).id shouldBe 4
    }
  }

  describe("DataFrameHelpers.removeSingletonGroups") {
    it("should remove groups of size 1") {
      val df = Seq(
        DummyRecord(1, "goup1", "id1"),
        DummyRecord(2, "goup1", "id2"),
        DummyRecord(3, "goup2", "id2")
      ).toDS

      val res = df.removeSingletonGroups.as[DummyRecord].collect

      res.length shouldBe 2
      res.head.ohubId shouldBe "id2"
    }
  }
}
