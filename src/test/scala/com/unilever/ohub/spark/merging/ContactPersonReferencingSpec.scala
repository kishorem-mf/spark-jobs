package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ContactPerson, TestContactPersons, TestOperators}
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.merging.ContactPersonReferencing.OHubIdAndRefId
import org.apache.spark.SparkException
import org.apache.spark.sql.Dataset

class ContactPersonReferencingSpec extends SparkJobSpec with TestContactPersons with TestOperators {
  import spark.implicits._

  private val operator1 = defaultOperatorWithSourceEntityId("1").copy(ohubId = Some("ohub-id-1"))
  private val operator2 = defaultOperatorWithSourceEntityId("2").copy(ohubId = Some("ohub-id-2"))

  private val contactPersons: Dataset[ContactPerson] = Seq(
    defaultContactPersonWithSourceEntityId("a").copy(operatorConcatId = operator1.concatId),
    defaultContactPersonWithSourceEntityId("b").copy(operatorConcatId = operator2.concatId)
  ).toDataset

  private val operators: Dataset[OHubIdAndRefId] = Seq(
    operator1,
    operator2,
    defaultOperatorWithSourceEntityId("3")
  ).toDataset
    .map(op ⇒ OHubIdAndRefId(op.ohubId, op.concatId))

  describe("ContactPersonReferencing.transform") {
    it("should set the right operator ohubId references from contact person to operator") {
      val result = ContactPersonReferencing.transform(spark, contactPersons, operators)

      val contactPersonResult = result.collect().toSeq
      contactPersonResult.size shouldBe 2

      val contactPerson1 = contactPersonResult.head
      contactPerson1.sourceEntityId shouldBe "a"
      contactPerson1.operatorConcatId shouldBe operator1.concatId
      contactPerson1.operatorOhubId shouldBe Some("ohub-id-1")

      val contactPerson2 = contactPersonResult(1)
      contactPerson2.sourceEntityId shouldBe "b"
      contactPerson2.operatorConcatId shouldBe operator2.concatId
      contactPerson2.operatorOhubId shouldBe Some("ohub-id-2")
    }

    it("should fail references from contact person to operator cannot be resolved") {
      intercept[SparkException] {
        val contactPerson3 = defaultContactPersonWithSourceEntityId("c").copy(operatorConcatId = "does-not-exist")

        ContactPersonReferencing.transform(spark, Seq(contactPerson3).toDataset, operators).head()
      }
    }
  }
}
