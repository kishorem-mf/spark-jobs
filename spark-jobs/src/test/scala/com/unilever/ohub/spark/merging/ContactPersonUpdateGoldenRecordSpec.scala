package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession._
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, TestContactPersons }
import com.unilever.ohub.spark.ingest.TestDomainDataProvider
import org.apache.spark.sql.Dataset

class ContactPersonUpdateGoldenRecordSpec extends SparkJobSpec with TestContactPersons {
  import spark.implicits._

  val SUT = ContactPersonUpdateGoldenRecord

  describe("ContactPersonUpdateGoldenRecord") {
    it("should favor Emakina contact person and mark it as a golden record") {
      val contactPersons: Dataset[ContactPerson] = Seq[ContactPerson](
        defaultContactPerson.copy(ohubId = Some("ohub-id-1"), sourceName = "EMAKINA"),
        defaultContactPerson.copy(ohubId = Some("ohub-id-1"), sourceName = "WEB_EVENT")
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
