package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.anonymization.{AnonymizeContactPersonJob, AnonymizedContactPersonIdentifier}
import com.unilever.ohub.spark.domain.entity.{ContactPerson, TestContactPersons}
import org.apache.spark.sql.functions.{col, sha2}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.Matchers

class AnonymizeContactPersonJobSpec extends SparkJobSpec with TestContactPersons with Matchers {


  private val SUT = AnonymizeContactPersonJob

  import spark.implicits._

  implicit val implicitSpark: SparkSession = spark


  def checkPersonalInformationHidden(persons: Array[ContactPerson], expectedRecords: Int = 1) = {
    assert(persons.length == expectedRecords)
    persons.foreach(cp => {
      assert(cp.firstName equals Some("HIDDEN"))
      assert(cp.lastName equals Some("HIDDEN"))
      assert(cp.emailAddress equals Some("HIDDEN"))
      assert(cp.mobileNumber equals Some("HIDDEN"))
      assert(cp.zipCode equals Some("HIDDEN"))
    })
  }

  def toAnonymized(emailAddress: Option[String], mobileNumber: Option[String]): Dataset[AnonymizedContactPersonIdentifier] =
    Seq((emailAddress.getOrElse(""), mobileNumber.getOrElse("")))
      .toDF("hashedEmailAddress", "hashedMobileNumber")
      .withColumn("hashedEmailAddress", sha2(col("hashedEmailAddress"), 256))
      .withColumn("hashedMobileNumber", sha2(col("hashedMobileNumber"), 256))
      .as[AnonymizedContactPersonIdentifier]

  describe("Contact person asked for right to be forgotten") {
    it("Should clean based on email and  mobile number we find a match") {
      val maskedContactPersons = toAnonymized(defaultContactPerson.emailAddress, defaultContactPerson.mobileNumber)
      val cpInput = Seq(defaultContactPerson).toDataset

      val transformed = SUT.transform(cpInput, maskedContactPersons)

      checkPersonalInformationHidden(transformed.collect())
    }

    it("Should clean based on email alone") {
      val maskedContactPersons = toAnonymized(defaultContactPerson.emailAddress, None)
      val cpInput = Seq(defaultContactPerson).toDataset

      val transformed = SUT.transform(cpInput, maskedContactPersons)

      checkPersonalInformationHidden(transformed.collect())
    }

    it("Should clean based on mobile alone") {
      val maskedContactPersons = toAnonymized(None, defaultContactPerson.mobileNumber)
      val cpInput = Seq(defaultContactPerson).toDataset

      val transformed = SUT.transform(cpInput, maskedContactPersons)

      checkPersonalInformationHidden(transformed.collect())
    }

    it("Should NOT clean if person did not ask to be forgotten") {
      val maskedContactPersons = Seq()
      val cpInput = Seq(defaultContactPerson).toDataset

      val result = SUT.transform(cpInput, spark.createDataset(maskedContactPersons)).collect()

      result.length shouldBe 1
      result(0) shouldBe defaultContactPerson
    }
  }

  describe("Contact person asked for right to be forgotten multiple matches") {
    it("Should clean based all contact persons based on email") {
      val maskedContactPersons = toAnonymized(defaultContactPerson.emailAddress, None)
      val cpInput = Seq(defaultContactPerson, defaultContactPerson.copy(firstName = Some("test"))).toDataset

      val result = SUT.transform(cpInput, maskedContactPersons)

      checkPersonalInformationHidden(result.collect(), 2)
    }
  }

}
