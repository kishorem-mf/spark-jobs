package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._

class ContactPersonUpdateEmailValidFlagSpecs extends SparkJobSpec with TestContactPersons {

  import spark.implicits._

  private val SUT = ContactPersonUpdateEmailValidFlag

  val invalidEmail = InvalidEmail("this@is.wrong")

  describe("Contact Person Update Email Valid Flag") {
    it("should mark emails present in invalidEmails as invalid in contactpersons") {
      val invalidContactPerson = defaultContactPerson.copy(emailAddress = Some(invalidEmail.emailAddress))
      val cpInput = Seq(invalidContactPerson).toDataset
      val ieInput = Seq(invalidEmail).toDataset

      val result = SUT.transform(spark, cpInput, ieInput).collect()

      result.length shouldBe 1
      result(0).isEmailAddressValid shouldBe Some(false)
    }

    it("should mark emails not present in invalidEmails as valid in contactpersons") {
      val validContactPerson = defaultContactPerson.copy(emailAddress = Some("perfectlyValid@ufs.com"))
      val cpInput = Seq(validContactPerson).toDataset
      val ieInput = Seq(invalidEmail).toDataset

      val result = SUT.transform(spark, cpInput, ieInput).collect()

      result.length shouldBe 1
      result(0).isEmailAddressValid shouldBe Some(true)
    }

    it("should be able to handle valid and invalid emails in the same dataset") {
      val invalidContactPerson = defaultContactPerson.copy(emailAddress = Some(invalidEmail.emailAddress))
      val validContactPerson = defaultContactPerson.copy(emailAddress = Some("perfectlyValid@ufs.com"))
      val cpInput = Seq(invalidContactPerson, validContactPerson).toDataset
      val ieInput = Seq(invalidEmail).toDataset

      val result = SUT.transform(spark, cpInput, ieInput).orderBy($"isEmailAddressValid".desc).collect()

      result.length shouldBe 2
      result(0).isEmailAddressValid shouldBe Some(true)
      result(1).isEmailAddressValid shouldBe Some(false)
    }

    it("should not create duplicate contactPersons if there are duplicate invalid emails ") {
      val invalidContactPerson = defaultContactPerson.copy(
        emailAddress = Some(invalidEmail.emailAddress),
        concatId = "AU~OHUB1~123")
      val validContactPerson = defaultContactPerson.copy(
        emailAddress = Some("perfectlyValid@ufs.com"),
        concatId = "AU~OHUB1~456")

      val cpInput = Seq(invalidContactPerson, validContactPerson).toDataset
      val ieInput = Seq(invalidEmail, invalidEmail).toDataset

      val result = SUT.transform(spark, cpInput, ieInput).orderBy($"isEmailAddressValid".desc).collect()

      result.length shouldBe 2
      result(0).isEmailAddressValid shouldBe Some(true)
      result(1).isEmailAddressValid shouldBe Some(false)

      result.count(_.concatId === "AU~OHUB1~123") shouldBe 1
    }
  }
}
