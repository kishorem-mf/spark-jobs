package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession._
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, TestContactPersons }
import com.unilever.ohub.spark.ingest.TestDomainDataProvider
import org.apache.spark.sql.Dataset

class ContactPersonUpdateGoldenRecordSpec extends SparkJobSpec with TestContactPersons {
  import spark.implicits._

  val SUT = ContactPersonUpdateGoldenRecord

  describe("ContactPersonUpdateGoldenRecord") {
    it("should favor Newest contact person based on dateCreated and mark it as a golden record") {
      val contactPersons: Dataset[ContactPerson] = Seq[ContactPerson](
        defaultContactPerson.copy(ohubId = Some("ohub-id-1"), sourceName = "EMAKINA",
          dateCreated = Some(new Timestamp(2l)),
          ohubUpdated = new Timestamp(1l)
        ),
        defaultContactPerson.copy(ohubId = Some("ohub-id-1"), sourceName = "WEB_EVENT",
          dateCreated = Some(new Timestamp(1l)),
          ohubUpdated = new Timestamp(2l)
        )
      ).toDataset

      val result = SUT
        .transform(spark, contactPersons, TestDomainDataProvider().sourcePreferences)
        .map(cp ⇒ (cp.ohubId, cp.sourceName, cp.isGoldenRecord))
        .collect().toSet

      result shouldBe Set(
        (Some("ohub-id-1"), "EMAKINA", true),
        (Some("ohub-id-1"), "WEB_EVENT", false)
      )
    }

    it("should favor Newest contact person based on ohubUpdated and mark it as a golden record") {
      val contactPersons: Dataset[ContactPerson] = Seq[ContactPerson](
        defaultContactPerson.copy(ohubId = Some("ohub-id-1"), sourceName = "EMAKINA",
          ohubUpdated = new Timestamp(2l)
        ),
        defaultContactPerson.copy(ohubId = Some("ohub-id-1"), sourceName = "WEB_EVENT",
          ohubUpdated = new Timestamp(1l)
        )
      ).toDataset

      val result = SUT
        .transform(spark, contactPersons, TestDomainDataProvider().sourcePreferences)
        .map(cp ⇒ (cp.ohubId, cp.sourceName, cp.isGoldenRecord))
        .collect().toSet

      result shouldBe Set(
        (Some("ohub-id-1"), "EMAKINA", true),
        (Some("ohub-id-1"), "WEB_EVENT", false)
      )
    }

    it("should favor Newest contact person based on dateUpdated and mark it as a golden record") {
      val contactPersons: Dataset[ContactPerson] = Seq[ContactPerson](
        defaultContactPerson.copy(ohubId = Some("ohub-id-1"), sourceName = "EMAKINA",
          dateUpdated = Some(new Timestamp(2l)),
          dateCreated = Some(new Timestamp(1l)),
          ohubUpdated = new Timestamp(1l)
        ),
        defaultContactPerson.copy(ohubId = Some("ohub-id-1"), sourceName = "WEB_EVENT",
          dateUpdated = Some(new Timestamp(1l)),
          dateCreated = Some(new Timestamp(2l)),
          ohubUpdated = new Timestamp(2l)
        )
      ).toDataset

      val result = SUT
        .transform(spark, contactPersons, TestDomainDataProvider().sourcePreferences)
        .map(cp ⇒ (cp.ohubId, cp.sourceName, cp.isGoldenRecord))
        .collect().toSet

      result shouldBe Set(
        (Some("ohub-id-1"), "EMAKINA", true),
        (Some("ohub-id-1"), "WEB_EVENT", false)
      )
    }

    it("should favor contact person based on isGoldenRecord and mark it as a golden record") {
      val contactPersons: Dataset[ContactPerson] = Seq[ContactPerson](
        defaultContactPerson.copy(ohubId = Some("ohub-id-1"), sourceName = "EMAKINA",
          dateUpdated = Some(new Timestamp(2l)),
          dateCreated = Some(new Timestamp(1l)),
          ohubUpdated = new Timestamp(1l),
          isGoldenRecord = true
        ),
        defaultContactPerson.copy(ohubId = Some("ohub-id-1"), sourceName = "WEB_EVENT",
          dateUpdated = Some(new Timestamp(2l)),
          dateCreated = Some(new Timestamp(1l)),
          ohubUpdated = new Timestamp(1l)
        )
      ).toDataset

      val result = SUT
        .transform(spark, contactPersons, TestDomainDataProvider().sourcePreferences)
        .map(cp ⇒ (cp.ohubId, cp.sourceName, cp.isGoldenRecord))
        .collect().toSet

      result shouldBe Set(
        (Some("ohub-id-1"), "EMAKINA", true),
        (Some("ohub-id-1"), "WEB_EVENT", false)
      )
    }
  }
}
