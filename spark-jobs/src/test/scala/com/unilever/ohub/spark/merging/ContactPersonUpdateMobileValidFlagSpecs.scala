package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{InvalidMobile, _}

class ContactPersonUpdateMobileValidFlagSpecs extends SparkJobSpec with TestContactPersons {

  import spark.implicits._

  private val SUT = ContactPersonUpdateMobileValidFlag

  val invalidMobile = InvalidMobile("0123456789")

  describe("Contact Person Update Mobile Valid Flag") {
    it("should mark mobiles present in invalidMobiles as invalid in contactpersons") {
      val invalidContactPerson = defaultContactPerson.copy(mobileNumber = Some(invalidMobile.mobileNumber))
      val cpInput = Seq(invalidContactPerson).toDataset
      val ieInput = Seq(invalidMobile).toDataset

      val result = SUT.transform(spark, cpInput, ieInput).collect()

      result.length shouldBe 1
      result(0).isMobileNumberValid shouldBe Some(false)
    }

    it("should mark mobiles not present in invalidMobiles as valid in contactpersons") {
      val validContactPerson = defaultContactPerson.copy(mobileNumber = Some("31293929192"))
      val cpInput = Seq(validContactPerson).toDataset
      val ieInput = Seq(invalidMobile).toDataset

      val result = SUT.transform(spark, cpInput, ieInput).collect()

      result.length shouldBe 1
      result(0).isMobileNumberValid shouldBe Some(true)
    }

    it("should be able to handle valid and invalid mobiles in the same dataset") {
      val invalidContactPerson = defaultContactPerson.copy(mobileNumber = Some(invalidMobile.mobileNumber))
      val validContactPerson = defaultContactPerson.copy(mobileNumber = Some("31293929192"))
      val cpInput = Seq(invalidContactPerson, validContactPerson).toDataset
      val ieInput = Seq(invalidMobile).toDataset

      val result = SUT.transform(spark, cpInput, ieInput).orderBy($"isMobileNumberValid".desc).collect()

      result.length shouldBe 2
      result(0).isMobileNumberValid shouldBe Some(true)
      result(1).isMobileNumberValid shouldBe Some(false)
    }

    it("should not create duplicate contactPersons if there are duplicate invalid mobiles ") {
      val invalidContactPerson = defaultContactPerson.copy(
        mobileNumber = Some(invalidMobile.mobileNumber),
        concatId = "AU~OHUB1~123")
      val validContactPerson = defaultContactPerson.copy(
        mobileNumber = Some("31293929192"),
        concatId = "AU~OHUB1~456")

      val cpInput = Seq(invalidContactPerson, validContactPerson).toDataset
      val ieInput = Seq(invalidMobile, invalidMobile).toDataset

      val result = SUT.transform(spark, cpInput, ieInput).orderBy($"isMobileNumberValid".desc).collect()

      result.length shouldBe 2
      result(0).isMobileNumberValid shouldBe Some(true)
      result(1).isMobileNumberValid shouldBe Some(false)

      result.count(_.concatId === "AU~OHUB1~123") shouldBe 1
    }
  }
}
