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
        defaultOperatorWithSourceName("sourceA").copy(dateCreated = Some(Timestamp.valueOf("2017-05-25 12:00:00"))),
        defaultOperatorWithSourceName("sourceB").copy(dateCreated = Some(Timestamp.valueOf("2017-04-25 12:00:00")))
      )
      val golden = Foo().pickGoldenRecord(sourcePreferences, operators)

      assert(golden.sourceName === "sourceA")
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
      val golden = Foo().pickGoldenRecord(sourcePreferences, operators)

      assert(golden.sourceName === "sourceB")

    }
  }
}
