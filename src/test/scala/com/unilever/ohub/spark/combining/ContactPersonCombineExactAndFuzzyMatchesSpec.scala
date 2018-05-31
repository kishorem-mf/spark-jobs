package com.unilever.ohub.spark.combining

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, TestContactPersons }
import com.unilever.ohub.spark.SharedSparkSession.spark
import org.apache.spark.sql.Dataset

class ContactPersonCombineExactAndFuzzyMatchesSpec extends SparkJobSpec with TestContactPersons {
  import spark.implicits._

  private val personA = defaultContactPersonWithSourceEntityId("a").copy(ohubCreated = Timestamp.valueOf("2018-05-30 20:50:00"), ohubUpdated = Timestamp.valueOf("2018-05-30 20:50:00"))

  private val personB1 = defaultContactPersonWithSourceEntityId("b").copy(ohubCreated = Timestamp.valueOf("2018-05-30 20:50:00"), ohubUpdated = Timestamp.valueOf("2018-05-30 20:50:00"))
  private val personB2 = defaultContactPersonWithSourceEntityId("b").copy(ohubCreated = Timestamp.valueOf("2018-05-29 20:50:00"), ohubUpdated = Timestamp.valueOf("2018-05-29 20:50:00"))

  private val personC1 = defaultContactPersonWithSourceEntityId("c").copy(ohubCreated = Timestamp.valueOf("2018-05-29 20:50:00"), ohubUpdated = Timestamp.valueOf("2018-05-29 20:50:00"))
  private val personC2 = defaultContactPersonWithSourceEntityId("c").copy(ohubCreated = Timestamp.valueOf("2018-05-30 20:50:00"), ohubUpdated = Timestamp.valueOf("2018-05-30 20:50:00"))

  private val personD1 = defaultContactPersonWithSourceEntityId("d").copy(ohubCreated = Timestamp.valueOf("2018-05-30 20:50:00"), ohubUpdated = Timestamp.valueOf("2018-05-30 20:50:00"))
  private val personD2 = defaultContactPersonWithSourceEntityId("d").copy(ohubCreated = Timestamp.valueOf("2018-05-29 20:50:00"), ohubUpdated = Timestamp.valueOf("2018-05-29 20:50:00"))

  private val personE = defaultContactPersonWithSourceEntityId("e").copy(ohubCreated = Timestamp.valueOf("2018-05-29 20:50:00"), ohubUpdated = Timestamp.valueOf("2018-05-29 20:50:00"))

  private val personF = defaultContactPersonWithSourceEntityId("f").copy(ohubCreated = Timestamp.valueOf("2018-05-30 20:50:00"), ohubUpdated = Timestamp.valueOf("2018-05-30 20:50:00"))

  private val contactPersonExactMatchedInput: Dataset[ContactPerson] =
    Seq(
      personA,
      personB1,
      personC1
    ).toDataset

  private val contactPersonFuzzyMatchedDeltaIntegratedInput: Dataset[ContactPerson] =
    Seq(
      personB2,
      personD1,
      personE
    ).toDataset

  private val contactPersonFuzzyMatchedDeltaLeftOversInput: Dataset[ContactPerson] =
    Seq(
      personC2,
      personD2,
      personF
    ).toDataset

  describe("ContactPersonCombineExactAndFuzzyMatches") {
    it("should combine exact and fuzzy matches correctly") {

      val result = ContactPersonCombineExactAndFuzzyMatches.transform(
        spark, contactPersonExactMatchedInput, contactPersonFuzzyMatchedDeltaIntegratedInput, contactPersonFuzzyMatchedDeltaLeftOversInput
      )

      result.map(_.sourceEntityId).collect().toSet shouldBe Set("a", "b", "c", "d", "e", "f")
    }
  }
}
