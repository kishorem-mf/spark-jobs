package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestContactPersons
import com.unilever.ohub.spark.ingest.TestDomainDataProvider
import org.apache.spark.sql.Dataset

class ContactPersonMatchingJoinerSpec extends SparkJobSpec with TestContactPersons {
  import spark.implicits._

  val SUT = ContactPersonMatchingJoiner

  private val matches: Dataset[MatchingResult] = Seq(
    MatchingResult("cp-1", "cp-2"),
    MatchingResult("cp-1", "cp-3"),
    MatchingResult("cp-5", "cp-6")
  ).toDataset
  private val contactPersons = Seq(
    defaultContactPerson.copy(concatId = "cp-1", sourceName = "a"),
    defaultContactPerson.copy(concatId = "cp-2", sourceName = "b"),
    defaultContactPerson.copy(concatId = "cp-3", sourceName = "c"),
    defaultContactPerson.copy(concatId = "cp-4", sourceName = "d"), // not in MATCHES
    defaultContactPerson.copy(concatId = "cp-5", sourceName = "x"),
    defaultContactPerson.copy(concatId = "cp-6", sourceName = "y")
  ).toDataset

  describe("ContactPersonMatchingJoiner") {
    it("should create ohub ids for all matched and unmatched contactpersons") {
      val sourcePreferences = Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "x" -> 4, "y" -> 5)

      val result = SUT.transform(spark, contactPersons, matches, TestDomainDataProvider(sourcePreferences = sourcePreferences))
        .collect
        .sortBy(_.sourceName)

      result.length shouldBe 6
      result.map(r ⇒ (r.sourceName, r.isGoldenRecord)) should contain inOrderOnly (
        ("a", true), ("b", false), ("c", false), ("d", true), ("x", true), ("y", false)
      )

      val firstGroup = result.take(3)
      firstGroup.map(_.ohubId) should contain only firstGroup.head.ohubId

      val secondGroup = result.drop(4)
      secondGroup.map(_.ohubId) should contain only secondGroup.head.ohubId
    }
  }
}
