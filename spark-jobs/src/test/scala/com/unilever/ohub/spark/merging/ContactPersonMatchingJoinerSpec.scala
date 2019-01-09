package com.unilever.ohub.spark.merging

import java.sql.Timestamp

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
    MatchingResult("cp-5", "cp-6"),
    MatchingResult("cp-7", "cp-8")
  ).toDataset
  private val contactPersons = Seq(
    // start Group 1
    defaultContactPerson.copy(concatId = "cp-1", sourceName = "a",
      dateUpdated = Some(new Timestamp(3L)), // Golden --> newest: DateUpdated is prefered for goldenrecord picking
      dateCreated = Some(new Timestamp(1L)),
      ohubUpdated = new Timestamp(1L)),
    defaultContactPerson.copy(concatId = "cp-2", sourceName = "b",
      dateUpdated = Some(new Timestamp(2L)),
      dateCreated = Some(new Timestamp(3L)),
      ohubUpdated = new Timestamp(3L)),
    defaultContactPerson.copy(concatId = "cp-3", sourceName = "c",
      dateUpdated = Some(new Timestamp(1L)),
      dateCreated = Some(new Timestamp(3L)),
      ohubUpdated = new Timestamp(3L)),
    // end Group 1
    defaultContactPerson.copy(concatId = "cp-4", sourceName = "d", dateUpdated = Some(new Timestamp(1L))), // not in MATCHES
    // start Group 2
    defaultContactPerson.copy(concatId = "cp-5", sourceName = "x",
      ohubUpdated = new Timestamp(3L)), // Golden --> newest
    defaultContactPerson.copy(concatId = "cp-6", sourceName = "y",
      ohubUpdated = new Timestamp(1L)),
    // end Group 2
    // start Group 3
    defaultContactPerson.copy(concatId = "cp-7", sourceName = "z1",
      dateCreated = Some(new Timestamp(1L)),
      ohubUpdated = new Timestamp(3L)),
    defaultContactPerson.copy(concatId = "cp-8", sourceName = "z2",
      dateCreated = Some(new Timestamp(3L)), // Golden --> newest dateCreated is preferred
      ohubUpdated = new Timestamp(1L))
    // end Group 3
  ).toDataset

  describe("ContactPersonMatchingJoiner") {
    it("should create ohub ids for all matched and unmatched contactpersons") {
      // SourcePreference is NOT USED AT THE MOMENT for goldenRecordmarking for contactpersons. It is expected to be used in the future.
      val sourcePreferences = Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "x" -> 4, "y" -> 5)

      val result = SUT.transform(spark, contactPersons, matches, TestDomainDataProvider(sourcePreferences = sourcePreferences))
        .collect
        .sortBy(_.sourceName)

      result.length shouldBe 8

      val firstGroup = result.take(3)
      firstGroup.map(_.ohubId) should contain only firstGroup.head.ohubId

      val unmatched = result.drop(3).head

      val secondGroup = result.drop(4).take(2)
      secondGroup.map(_.ohubId) should contain only secondGroup.head.ohubId

      val thirdGroup = result.drop(6).take(2)
      thirdGroup.map(_.ohubId) should contain only thirdGroup.head.ohubId

      Set(firstGroup.head.ohubId, secondGroup.head.ohubId, thirdGroup.head.ohubId, unmatched.ohubId).size shouldBe 4
    }

    it("should mark the newest contactpersons within a group golden") {
      // SourcePreference is NOT USED AT THE MOMENT for goldenRecordmarking for contactpersons. It is expected to be used in the future.
      val sourcePreferences = Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "x" -> 4, "y" -> 5, "z1" -> 1, "z2" -> 2)

      val result = SUT.transform(spark, contactPersons, matches, TestDomainDataProvider(sourcePreferences = sourcePreferences))
        .collect
        .sortBy(_.sourceName)

      result.map(r ⇒ (r.sourceName, r.isGoldenRecord)) should contain inOrderOnly(
        ("a", true), ("b", false), ("c", false), ("d", true), ("x", true), ("y", false), ("z1", false), ("z2", true)
      )
    }
  }
}
