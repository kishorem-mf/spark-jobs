package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ Operator, TestOperators }

class GoldenRecordPickingSpec extends SparkJobSpec with TestOperators {

  case class Foo() extends GoldenRecordPicking[Operator]

  describe("picking golden record") {
    it("should pick the operator with highest sourcePreference") {
      val sourcePreferences = Map(
        "sourceA" -> 2,
        "sourceB" -> 1
      )
      val operators = Seq(
        defaultOperatorWithSourceName("sourceA"),
        defaultOperatorWithSourceName("sourceB")
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
        defaultOperatorWithSourceName("sourceA").copy(dateUpdated = Option.empty, dateCreated = Some(Timestamp.valueOf("2017-05-25 12:00:00"))),
        defaultOperatorWithSourceName("sourceB").copy(dateUpdated = Option.empty, dateCreated = Some(Timestamp.valueOf("2017-04-25 12:00:00")))
      )
      val golden = Foo().pickGoldenRecord(sourcePreferences, operators)

      assert(golden.sourceName === "sourceA")
    }

    it("should pick the operator updated date if not null if sourcePreferences are equal") {
      val sourcePreferences = Map(
        "sourceA" -> 1,
        "sourceB" -> 1
      )
      val operators = Seq(
        defaultOperatorWithSourceName("sourceA").copy(dateUpdated = Some(Timestamp.valueOf("2017-04-26 12:00:00")), dateCreated = Some(Timestamp.valueOf("2017-04-24 12:00:00"))),
        defaultOperatorWithSourceName("sourceB").copy(dateUpdated = Some(Timestamp.valueOf("2017-04-26 12:00:00")), dateCreated = Some(Timestamp.valueOf("2017-04-25 12:00:00")))
      )
      val golden = Foo().pickGoldenRecord(sourcePreferences, operators)

      assert(golden.sourceName === "sourceB")
    }


    it("should pick the operator that was already golden when dateCreated and sourcePreferences are equal") {
      val sourcePreferences = Map(
        "sourceA" -> 1,
        "sourceB" -> 1
      )

      val date = Timestamp.valueOf("2017-05-25 12:00:00");
      val operators = Seq(
        defaultOperatorWithSourceName("sourceA").copy(dateCreated = Some(date)),
        defaultOperatorWithSourceName("sourceB").copy(dateCreated = Some(date), isGoldenRecord = true)
      )
      var golden = Foo().pickGoldenRecord(sourcePreferences, operators)
      assert(golden.sourceName === "sourceB")

      // The location of the golden record shouldn't matter, so reversed should give the same results
      val goldenReversed = Foo().pickGoldenRecord(sourcePreferences, operators.reverse)
      assert(goldenReversed.sourceName === "sourceB")
    }
  }
}
