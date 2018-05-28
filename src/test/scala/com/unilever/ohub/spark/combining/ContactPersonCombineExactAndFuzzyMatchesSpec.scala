package com.unilever.ohub.spark.combining

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, TestContactPersons }
import com.unilever.ohub.spark.SharedSparkSession.spark
import org.apache.spark.sql.Dataset

class ContactPersonCombineExactAndFuzzyMatchesSpec extends SparkJobSpec with TestContactPersons {
  import spark.implicits._

  private val updatedExactMatchInput: Dataset[ContactPerson] =
    Seq(
      defaultContactPersonWithSourceEntityId("a"),
      defaultContactPersonWithSourceEntityId("b")
    ).toDataset

  private val ingestedExactMatchInput: Dataset[ContactPerson] =
    Seq(
      defaultContactPersonWithSourceEntityId("c"),
      defaultContactPersonWithSourceEntityId("d")
    ).toDataset

  private val fuzzyMatchCombinedInputFile: Dataset[ContactPerson] =
    Seq(
      defaultContactPersonWithSourceEntityId("a"),
      defaultContactPersonWithSourceEntityId("c"),
      defaultContactPersonWithSourceEntityId("e")
    ).toDataset

  describe("ContactPersonCombineExactAndFuzzyMatches") {
    it("should combine exact and fuzzy matches correctly") {

      val result = ContactPersonCombineExactAndFuzzyMatches.transform(spark, updatedExactMatchInput, ingestedExactMatchInput, fuzzyMatchCombinedInputFile)

      result.map(_.sourceEntityId).collect().toSet shouldBe Set("a", "b", "c", "d", "e")
    }
  }
}
