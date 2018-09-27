package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, TestContactPersons }
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.ingest.TestDomainDataProvider
import org.apache.spark.sql.Dataset

class ContactPersonExactMatcherSpec extends SparkJobSpec with TestContactPersons {
  import spark.implicits._

  /*
    Note: for the ContactPersonExactMatcher spark job only when there is an exact match on the concatenated string of emailAddress & mobileNumber
    then contact persons match, hence the expected results below. This is a business decision and thus implemented accordingly.
   */

  private val ingestedContactPersons: Dataset[ContactPerson] = Seq(
    defaultContactPersonWithSourceEntityId("a"),
    defaultContactPersonWithSourceEntityId("b").copy(mobileNumber = None),
    defaultContactPersonWithSourceEntityId("c").copy(emailAddress = None),
    defaultContactPersonWithSourceEntityId("d").copy(emailAddress = None, mobileNumber = None), // this one should be filtered from the result
    defaultContactPersonWithSourceEntityId("e"), // this one matches with a
    defaultContactPersonWithSourceEntityId("f").copy(mobileNumber = None), // this one matches with b
    defaultContactPersonWithSourceEntityId("g").copy(emailAddress = None), // this one matches with c
    // all other contact persons don't match
    defaultContactPersonWithSourceEntityId("v").copy(emailAddress = Some("something@another.server"), mobileNumber = Some("12345678")),
    defaultContactPersonWithSourceEntityId("w").copy(emailAddress = Some("something@another.server")),
    defaultContactPersonWithSourceEntityId("x").copy(mobileNumber = Some("12345678")),
    defaultContactPersonWithSourceEntityId("y").copy(emailAddress = Some("something@another.server"), mobileNumber = None),
    defaultContactPersonWithSourceEntityId("z").copy(emailAddress = None, mobileNumber = Some("12345678"))
  ).toDataset

  describe("ContactPersonMerging.transform") {
    it("should group all contact persons with the same email address and mobile phone number") {
      val result: Dataset[ContactPerson] = ContactPersonExactMatcher.transform(
        spark, ingestedContactPersons, TestDomainDataProvider().sourcePreferences
      )

      result.map(_.sourceEntityId).collect().toSet shouldBe Set("a", "b", "c", "e", "f", "g", "v", "w", "x", "y", "z")
      result.filter(_.isGoldenRecord).map(_.sourceEntityId).collect().toSet shouldBe Set("e", "f", "g", "v", "w", "x", "y", "z")

      result.filter(cp ⇒ cp.sourceEntityId == "a" || cp.sourceEntityId == "e").map(_.ohubId).distinct().count() shouldBe 1
      result.filter(cp ⇒ cp.sourceEntityId == "b" || cp.sourceEntityId == "f").map(_.ohubId).distinct().count() shouldBe 1
      result.filter(cp ⇒ cp.sourceEntityId == "c" || cp.sourceEntityId == "g").map(_.ohubId).distinct().count() shouldBe 1
    }

    it("should determine the left-overs correctly, given a set of exact matches") {
      val exactMatchContact = ingestedContactPersons.filter(cp ⇒ cp.sourceEntityId != "d")

      val result: Dataset[ContactPerson] = ContactPersonExactMatcher.leftOversForFuzzyMatching(
        spark, ingestedContactPersons, exactMatchContact
      )

      result.map(_.sourceEntityId).collect().toSet shouldBe Set("d")
    }
  }
}
