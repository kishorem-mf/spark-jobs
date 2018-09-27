package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.dispatcher.model.DispatcherContactPerson
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, TestContactPersons }
import org.apache.spark.sql.Dataset
import cats.syntax.option._

class ContactPersonDispatcherConverterSpec extends SparkJobSpec with TestContactPersons {

  private val contactPersonConverter = ContactPersonDispatcherConverter

  describe("contact person dispatcher converter") {
    it("should convert a domain contact person correctly into an dispatcher contact person") {
      import spark.implicits._

      /**
       * Input file containing ContactPerson records
       */
      val input: Dataset[ContactPerson] = {
        spark.createDataset(
          List(defaultContactPerson)
            .map(_.copy(isGoldenRecord = true))
            .map(_.copy(ohubId = "randomId"))
        )
      }

      /**
       * There is no previous run containing ContactPerson records, so
       * we create an empty dataset
       */
      val emptyDataset: Dataset[ContactPerson] = spark.emptyDataset[ContactPerson]

      /**
       * Transformed DispatcherContactPerson
       */
      val result: List[DispatcherContactPerson] = {
        contactPersonConverter.transform(spark, input, emptyDataset).collect().toList
      }

      result should contain(DispatcherContactPerson(
        DATE_OF_BIRTH = "1975-12-21 00:00:00",
        CITY = "Melbourne",
        CP_ORIG_INTEGRATION_ID = "AU~WUFOO~AB123",
        COUNTRY_CODE = "AU",
        COUNTRY = "Australia",
        EMAIL_ADDRESS = "jwilliams@downunder.au",
        CONFIRMED_OPT_IN_DATE = "2015-09-30 14:23:03",
        OPT_IN_DATE = "2015-09-30 14:23:02",
        FAX_NUMBER = "61396621812",
        GENDER = "M",
        DM_OPT_OUT = true,
        CONFIRMED_OPT_IN = true,
        OPT_IN = true,
        EMAIL_OPT_OUT = true,
        FAX_OPT_OUT = true,
        FIRST_NAME = "John",
        MOB_CONFIRMED_OPT_IN = true,
        MOB_OPT_IN = true,
        MOBILE_OPT_OUT = true,
        FIXED_OPT_OUT = true,
        HOUSE_NUMBER = "443",
        HOUSE_NUMBER_ADD = "A",
        DELETE_FLAG = false,
        GOLDEN_RECORD_FLAG = true,
        KEY_DECISION_MAKER = true,
        PREFERRED = true,
        LANGUAGE = "en",
        LAST_NAME = "Williams",
        MOB_CONFIRMED_OPT_IN_DATE = "2015-09-30 14:23:05",
        MOBILE_PHONE_NUMBER = "61612345678",
        MOB_OPT_IN_DATE = "2015-09-30 14:23:04",
        CREATED_AT = "2015-06-30 13:49:00",
        CP_LNKD_INTEGRATION_ID = "randomId",
        UPDATED_AT = "2015-06-30 13:50:00",
        OPR_ORIG_INTEGRATION_ID = "AU~WUFOO~E1-1234",
        FIXED_PHONE_NUMBER = "61396621811",
        SOURCE_ID = "AB123",
        SOURCE = "WUFOO",
        SCM = "Mobile",
        STATE = "Alabama",
        STREET = "Highstreet",
        TITLE = "Mr",
        ZIP_CODE = "2057",
        MIDDLE_NAME = none,
        ROLE = "Chef",
        ORG_FIRST_NAME = none,
        ORG_LAST_NAME = none,
        ORG_EMAIL_ADDRESS = none,
        ORG_FIXED_PHONE_NUMBER = none,
        ORG_MOBILE_PHONE_NUMBER = none,
        ORG_FAX_NUMBER = none,
        MOB_OPT_OUT_DATE = none
      ))
    }
  }
}
