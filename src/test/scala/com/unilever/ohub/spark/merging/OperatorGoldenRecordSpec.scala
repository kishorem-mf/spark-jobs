package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJobSpec

class OperatorGoldenRecordSpec extends SparkJobSpec {

  case class Foo() extends OperatorGoldenRecord

  describe("picking golden record") {
    it("should pick the operator with highest sourcePreference") {
      val sourcePreferences = Map(
        "sourceA" -> 2,
        "sourceB" -> 1
      )
      val operators = Seq(
        defaultOperatorRecord.copy(sourceName = "sourceA"),
        defaultOperatorRecord.copy(sourceName = "sourceB")
      )
      val golden = Foo().pickGoldenRecord(sourcePreferences, operators)

      assert(golden.sourceName === "sourceB")

    }

    it("should pick the operator created last if sourcePreferences are equal") {
      val sourcePreferences = Map(
        "sourceA" -> 1,
        "sourceB" -> 1
      )
      val operators = Seq(
        defaultOperatorRecord.copy(sourceName = "sourceA", dateCreated = Some(Timestamp.valueOf("2017-05-25 12:00:00"))),
        defaultOperatorRecord.copy(sourceName = "sourceB", dateCreated = Some(Timestamp.valueOf("2017-04-25 12:00:00")))
      )
      val golden = Foo().pickGoldenRecord(sourcePreferences, operators)

      assert(golden.sourceName === "sourceA")
    }
  }

}
