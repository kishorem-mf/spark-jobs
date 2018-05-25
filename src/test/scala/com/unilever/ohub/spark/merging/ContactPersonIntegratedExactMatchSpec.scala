package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, TestContactPersons }
import org.apache.spark.sql.Dataset

class ContactPersonIntegratedExactMatchSpec extends SparkJobSpec with TestContactPersons {
  import spark.implicits._

  private val contactPersonA = defaultContactPersonWithSourceEntityId("a").copy(isGoldenRecord = true, ohubId = Some("ohub-a"))
  private val contactPersonB = defaultContactPersonWithSourceEntityId("b").copy(isGoldenRecord = true, ohubId = Some("ohub-b"))
  private val contactPersonC = defaultContactPersonWithSourceEntityId("c").copy(isGoldenRecord = true, ohubId = Some("ohub-c"))

  private val integratedContactPersons: Dataset[ContactPerson] = Seq(
    contactPersonA,
    contactPersonB.copy(mobileNumber = None),
    contactPersonC.copy(emailAddress = None),
    defaultContactPersonWithSourceEntityId("d").copy(isGoldenRecord = true, ohubId = Some("ohub-d"), emailAddress = None, mobileNumber = None), // this one is on it's own
    defaultContactPersonWithSourceEntityId("e").copy(ohubId = contactPersonA.ohubId), // this one matches with a
    defaultContactPersonWithSourceEntityId("f").copy(mobileNumber = None, ohubId = contactPersonB.ohubId), // this one matches with b
    defaultContactPersonWithSourceEntityId("g").copy(emailAddress = None, ohubId = contactPersonC.ohubId) // this one matches with c
  ).toDataset

  private val deltaContactPersons: Dataset[ContactPerson] = Seq(
    contactPersonA.copy(ohubId = None, firstName = Some("updated-first-name-a")), // should be in integrated with the ohubId of former a
    defaultContactPersonWithSourceEntityId("h").copy(ohubId = None), // should be in integrated with ohubId of a
    defaultContactPersonWithSourceEntityId("i").copy(ohubId = None, mobileNumber = None), // should be in integrated with ohubId of b
    // new contact persons
    defaultContactPersonWithSourceEntityId("v").copy(ohubId = None, emailAddress = Some("something@another.server"), mobileNumber = Some("12345678")), // should be in unmatched
    defaultContactPersonWithSourceEntityId("w").copy(ohubId = None, emailAddress = None, mobileNumber = None) // should be in unmatched
  ).toDataset

  describe("ContactPersonIntegratedExactMatch.transform") {
    it("should exact match integrated vs new daily delta") {
      val (updatedIntegrated: Dataset[ContactPerson], unmatchedDelta: Dataset[ContactPerson]) = ContactPersonIntegratedExactMatch.transform(
        spark, integratedContactPersons, deltaContactPersons
      )

      updatedIntegrated.map(cp ⇒ (cp.sourceEntityId, cp.isGoldenRecord, cp.ohubId, cp.firstName)).collect().toSet shouldBe
        Set(
          ("a", true, Some("ohub-a"), Some("updated-first-name-a")),
          ("b", true, Some("ohub-b"), Some("John")),
          ("c", true, Some("ohub-c"), Some("John")),
          ("d", true, Some("ohub-d"), Some("John")),
          ("e", false, Some("ohub-a"), Some("John")),
          ("f", false, Some("ohub-b"), Some("John")),
          ("g", false, Some("ohub-c"), Some("John")),
          ("h", false, Some("ohub-a"), Some("John")),
          ("i", false, Some("ohub-b"), Some("John"))
        )

      unmatchedDelta.map(cp ⇒ (cp.sourceEntityId, cp.isGoldenRecord, cp.ohubId, cp.firstName)).collect().toSet shouldBe
        Set(
          ("v", false, None, Some("John")),
          ("w", false, None, Some("John"))
        )
    }
  }
}
