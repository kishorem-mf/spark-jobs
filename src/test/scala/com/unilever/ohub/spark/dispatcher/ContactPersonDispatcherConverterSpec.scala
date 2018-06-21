package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.dispatcher.model.DispatcherContactPerson
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, TestContactPersons }
import org.apache.spark.sql.Dataset

class ContactPersonDispatcherConverterSpec extends SparkJobSpec with TestContactPersons {

  private val contactPersonConverter = ContactPersonDispatcherConverter

  describe("contact person dispatcher delta converter") {
    it("should convert a domain contact person correctly into an dispatcher converter containing only delta records") {
      import spark.implicits._

      /**
       * Input file containing ContactPerson records
       */
      val input: Dataset[ContactPerson] = {
        spark.createDataset(
          List(defaultContactPerson)
            .map(_.copy(isGoldenRecord = true))
            .map(_.copy(ohubId = Some("randomId")))
        )
      }

      val emptyDataset: Dataset[ContactPerson] = spark.emptyDataset[ContactPerson]

      /**
       * Transformed DispatcherContactPerson
       */
      val result: List[DispatcherContactPerson] = {
        contactPersonConverter.transform(spark, input, emptyDataset).collect().toList
      }

      result should contain(DispatcherContactPerson(
        DATE_OF_BIRTH = Some("1975-12-21 00:00:00"),
        CITY = Some("Melbourne"),
        CP_ORIG_INTEGRATION_ID = "AU~WUFOO~AB123",
        COUNTRY_CODE = "AU",
        COUNTRY = Some("Australia"),
        EMAIL_ADDRESS = Some("jwilliams@downunder.au"),
        CONFIRMED_OPT_IN_DATE = Some("2015-09-30 14:23:03"),
        OPT_IN_DATE = Some("2015-09-30 14:23:02"),
        FAX_NUMBER = Some("61396621812"),
        GENDER = Some("M"),
        DM_OPT_OUT = Some(true),
        CONFIRMED_OPT_IN = Some(true),
        OPT_IN = Some(true),
        EMAIL_OPT_OUT = Some(true),
        FAX_OPT_OUT = Some(true),
        MOB_CONFIRMED_OPT_IN = Some(true),
        MOB_OPT_IN = Some(true),
        MOBILE_OPT_OUT = Some(true),
        FIXED_OPT_OUT = Some(true),
        HOUSE_NUMBER = Some("443"),
        HOUSE_NUMBER_ADD = Some("A"),
        DELETE_FLAG = false,
        GOLDEN_RECORD_FLAG = true,
        KEY_DECISION_MAKER = Some(true),
        PREFERRED = Some(true),
        LANGUAGE = Some("en"),
        LAST_NAME = Some("Williams"),
        MOB_CONFIRMED_OPT_IN_DATE = Some("2015-09-30 14:23:05"),
        MOBILE_PHONE_NUMBER = Some("61612345678"),
        MOB_OPT_IN_DATE = Some("2015-09-30 14:23:04"),
        CREATED_AT = "2015-06-30 13:49:00",
        CP_LNKD_INTEGRATION_ID = Some("randomId"),
        UPDATED_AT = "2015-06-30 13:50:00",
        OPR_ORIG_INTEGRATION_ID = "AU~WUFOO~E1-1234",
        FIXED_PHONE_NUMBER = Some("61396621811"),
        SOURCE_ID = "AB123",
        SOURCE = "WUFOO",
        SCM = Some("Mobile"),
        STATE = Some("Alabama"),
        STREET = Some("Highstreet"),
        TITLE = Some("Mr"),
        ZIP_CODE = Some("2057"),
        MIDDLE_NAME = None,
        ROLE = Some("Chef"),
        ORG_FIRST_NAME = None,
        ORG_LAST_NAME = None,
        ORG_EMAIL_ADDRESS = None,
        ORG_FIXED_PHONE_NUMBER = None,
        ORG_MOBILE_PHONE_NUMBER = None,
        ORG_FAX_NUMBER = None,
        MOB_OPT_OUT_DATE = None
      ))
    }
  }
}
