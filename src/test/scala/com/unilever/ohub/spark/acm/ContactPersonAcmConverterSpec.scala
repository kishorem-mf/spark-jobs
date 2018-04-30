package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.acm.model.UFSRecipient
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, TestContactPersons }
import org.apache.spark.sql.Dataset

class ContactPersonAcmConverterSpec extends SparkJobSpec with TestContactPersons {

  private[acm] val SUT = ContactPersonAcmConverter

  describe("contact person acm converter") {
    it("should convert a domain operator correctly into an acm converter") {
      import spark.implicits._

      val input: Dataset[ContactPerson] = spark.createDataset(Seq(defaultContactPerson.copy(isGoldenRecord = true)))
      val result = SUT.transform(spark, input)

      result.count() shouldBe 1

      val actualAcmContactPerson = result.head()
      val expectedAcmContactPerson =
        UFSRecipient(
          CP_ORIG_INTEGRATION_ID = "AU~WUFOO~AB123",
          CP_LNKD_INTEGRATION_ID = defaultContactPerson.ohubId.get,
          OPR_ORIG_INTEGRATION_ID = Some("G1234"),
          GOLDEN_RECORD_FLAG = "Y",
          WEB_CONTACT_ID = "",
          EMAIL_OPTOUT = Some("Y"),
          PHONE_OPTOUT = Some("Y"),
          FAX_OPTOUT = Some("Y"),
          MOBILE_OPTOUT = Some("Y"),
          DM_OPTOUT = Some("Y"),
          LAST_NAME = "Williams",
          FIRST_NAME = "John",
          MIDDLE_NAME = "",
          TITLE = Some("Mr"),
          GENDER = Some("1"),
          LANGUAGE = Some("en"),
          EMAIL_ADDRESS = Some("jwilliams@downunder.au"),
          MOBILE_PHONE_NUMBER = Some("61612345678"),
          PHONE_NUMBER = Some("61396621811"),
          FAX_NUMBER = Some("61396621812"),
          STREET = Some("Highstreet"),
          HOUSENUMBER = "443 A",
          ZIPCODE = Some("2057"),
          CITY = Some("Melbourne"),
          COUNTRY = Some("Australia"),
          DATE_CREATED = Some("2015-06-30 13:47:00"),
          DATE_UPDATED = Some("2015-06-30 13:48:00"),
          DATE_OF_BIRTH = Some("1975-12-21 00:00:00"),
          PREFERRED = Some("Y"),
          ROLE = Some("Chef"),
          COUNTRY_CODE = Some("AU"),
          SCM = Some("Mobile"),
          DELETE_FLAG = Some("0"),
          KEY_DECISION_MAKER = Some("Y"),
          OPT_IN = Some("Y"),
          OPT_IN_DATE = Some("2015-09-30 14:23:02"),
          CONFIRMED_OPT_IN = Some("Y"),
          CONFIRMED_OPT_IN_DATE = Some("2015-09-30 14:23:01"),
          MOB_OPT_IN = Some("Y"),
          MOB_OPT_IN_DATE = Some("2015-09-30 14:23:04"),
          MOB_CONFIRMED_OPT_IN = Some("Y"),
          MOB_CONFIRMED_OPT_IN_DATE = Some("2015-09-30 14:23:05"),
          MOB_OPT_OUT_DATE = "",
          ORG_FIRST_NAME = Some("John"),
          ORG_LAST_NAME = Some("Williams"),
          ORG_EMAIL_ADDRESS = Some("jwilliams@downunder.au"),
          ORG_FIXED_PHONE_NUMBER = Some("61396621811"),
          ORG_MOBILE_PHONE_NUMBER = Some("61612345678"),
          ORG_FAX_NUMBER = Some("61396621812")
        )

      actualAcmContactPerson shouldBe expectedAcmContactPerson
    }
  }
}
