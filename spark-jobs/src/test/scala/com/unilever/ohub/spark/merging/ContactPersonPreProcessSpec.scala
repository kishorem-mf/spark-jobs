package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, TestContactPersons }
import org.apache.spark.sql.Dataset

class ContactPersonPreProcessSpec extends SparkJobSpec with TestContactPersons {
  import spark.implicits._

  private val personA = defaultContactPersonWithSourceEntityId("a").copy(ohubCreated = Timestamp.valueOf("2018-05-30 20:50:00"), ohubUpdated = Timestamp.valueOf("2018-05-30 20:50:00"))

  private val personB1 = defaultContactPersonWithSourceEntityId("b").copy(ohubCreated = Timestamp.valueOf("2018-05-30 20:50:00"), ohubUpdated = Timestamp.valueOf("2018-05-30 20:50:00"))
  private val personB2 = defaultContactPersonWithSourceEntityId("b").copy(ohubCreated = Timestamp.valueOf("2018-05-29 20:50:00"), ohubUpdated = Timestamp.valueOf("2018-05-29 20:50:00"))

  private val personC1 = defaultContactPersonWithSourceEntityId("c").copy(ohubCreated = Timestamp.valueOf("2018-05-29 20:50:00"), ohubUpdated = Timestamp.valueOf("2018-05-29 20:50:00"))
  private val personC2 = defaultContactPersonWithSourceEntityId("c").copy(ohubCreated = Timestamp.valueOf("2018-05-30 20:50:00"), ohubUpdated = Timestamp.valueOf("2018-05-30 20:50:00"))

  private val personD = defaultContactPersonWithSourceEntityId("d").copy(ohubCreated = Timestamp.valueOf("2018-05-29 20:50:00"), ohubUpdated = Timestamp.valueOf("2018-05-29 20:50:00"))

  private val contactPersonIntegratedInput: Dataset[ContactPerson] =
    Seq(
      personB1,
      personC1,
      personD
    ).toDataset

  private val contactPersonDeltaInput: Dataset[ContactPerson] =
    Seq(
      personA,
      personB2,
      personC2
    ).toDataset

  describe("ContactPersonPreProcess") {
    it("should pre process contact persons correctly") {
      val result = ContactPersonPreProcess.transform(
        spark, contactPersonIntegratedInput, contactPersonDeltaInput
      )

      result.count() shouldBe 3

      result.map(cp ⇒ (cp.sourceEntityId, cp.ohubCreated, cp.ohubUpdated)).collect.toSet shouldBe
        Set(
          ("a", Timestamp.valueOf("2018-05-30 20:50:00"), Timestamp.valueOf("2018-05-30 20:50:00")),
          ("b", Timestamp.valueOf("2018-05-29 20:50:00"), Timestamp.valueOf("2018-05-29 20:50:00")),
          ("c", Timestamp.valueOf("2018-05-29 20:50:00"), Timestamp.valueOf("2018-05-30 20:50:00"))
        )
    }
  }
}
