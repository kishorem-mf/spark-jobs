package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.storage.CountryRecord
import com.unilever.ohub.spark.SharedSparkSession.spark

class ContactPersonConverterTest extends SparkJobSpec {
  private val defaultContactPersonRecord = ContactPersonRecord(
    CONTACT_PERSON_CONCAT_ID = "",
    REF_CONTACT_PERSON_ID = Some(""),
    SOURCE = Some("Source"),
    COUNTRY_CODE = Some(""),
    STATUS = Some(true),
    STATUS_ORIGINAL = Some("true"),
    REF_OPERATOR_ID = Some(""),
    CP_INTEGRATION_ID = Some(""),
    DATE_CREATED = Some(new Timestamp(1)),
    DATE_MODIFIED = Some(new Timestamp(1)),
    FIRST_NAME = Some(""),
    FIRST_NAME_CLEANSED = Some(""),
    LAST_NAME = Some(""),
    LAST_NAME_CLEANSED = Some(""),
    BOTH_NAMES_CLEANSED = Some(""),
    TITLE = Some(""),
    GENDER= Some(""),
    FUNCTION= Some(""),
    LANGUAGE_KEY= Some(""),
    BIRTH_DATE= Some(new Timestamp(1)),
    STREET= Some(""),
    STREET_CLEANSED= Some(""),
    HOUSENUMBER= Some(""),
    HOUSENUMBER_EXT= Some(""),
    CITY= Some(""),
    CITY_CLEANSED= Some(""),
    ZIP_CODE= Some(""),
    ZIP_CODE_CLEANSED= Some(""),
    STATE = Some(""),
    COUNTRY = Some("Foo"),
    PREFERRED_CONTACT= Some(true),
    PREFERRED_CONTACT_ORIGINAL= Some(""),
    KEY_DECISION_MAKER= Some(true),
    KEY_DECISION_MAKER_ORIGINAL= Some(""),
    SCM= Some(""),
    EMAIL_ADDRESS= Some(""),
    EMAIL_ADDRESS_ORIGINAL= Some(""),
    PHONE_NUMBER= Some(""),
    PHONE_NUMBER_ORIGINAL= Some(""),
    MOBILE_PHONE_NUMBER= Some(""),
    MOBILE_PHONE_NUMBER_ORIGINAL= Some(""),
    FAX_NUMBER= Some(""),
    OPT_OUT= Some(true),
    OPT_OUT_ORIGINAL= Some(""),
    REGISTRATION_CONFIRMED= Some(true),
    REGISTRATION_CONFIRMED_ORIGINAL= Some(""),
    REGISTRATION_CONFIRMED_DATE= Some(new Timestamp(1)),
    REGISTRATION_CONFIRMED_DATE_ORIGINAL= Some(""),
    EM_OPT_IN= Some(true),
    EM_OPT_IN_ORIGINAL= Some(""),
    EM_OPT_IN_DATE= Some(new Timestamp(1)),
    EM_OPT_IN_DATE_ORIGINAL= Some(""),
    EM_OPT_IN_CONFIRMED= Some(true),
    EM_OPT_IN_CONFIRMED_ORIGINAL= Some(""),
    EM_OPT_IN_CONFIRMED_DATE= Some(new Timestamp(1)),
    EM_OPT_IN_CONFIRMED_DATE_ORIGINAL= Some(""),
    EM_OPT_OUT= Some(true),
    EM_OPT_OUT_ORIGINAL= Some(""),
    DM_OPT_IN= Some(true),
    DM_OPT_IN_ORIGINAL= Some(""),
    DM_OPT_OUT= Some(true),
    DM_OPT_OUT_ORIGINAL= Some(""),
    TM_OPT_IN= Some(true),
    TM_OPT_IN_ORIGINAL= Some(""),
    TM_OPT_OUT= Some(true),
    TM_OPT_OUT_ORIGINAL= Some(""),
    MOB_OPT_IN= Some(true),
    MOB_OPT_IN_ORIGINAL= Some(""),
    MOB_OPT_IN_DATE= Some(new Timestamp(1)),
    MOB_OPT_IN_DATE_ORIGINAL= Some(""),
    MOB_OPT_IN_CONFIRMED= Some(true),
    MOB_OPT_IN_CONFIRMED_ORIGINAL= Some(""),
    MOB_OPT_IN_CONFIRMED_DATE= Some(new Timestamp(1)),
    MOB_OPT_IN_CONFIRMED_DATE_ORIGINAL= Some(""),
    MOB_OPT_OUT= Some(true),
    MOB_OPT_OUT_ORIGINAL= Some(""),
    FAX_OPT_IN= Some(true),
    FAX_OPT_IN_ORIGINAL= Some(""),
    FAX_OPT_OUT= Some(true),
    FAX_OPT_OUT_ORIGINAL= Some("")
  )

  import spark.implicits._

  describe("The ContactPersonConverter job's") {
    describe("transform function") {
      describe("when given a ContactPersonRecord with COUNTRY_CODE 'NL'") {
        val contactPersonRecord = defaultContactPersonRecord.copy(
          COUNTRY_CODE = Some("NL")
        )

        describe("and given a CountryRecord with COUNTRY_CODE 'NL'") {
          val countryRecord = CountryRecord(
            COUNTRY_CODE = "NL",
            COUNTRY = "Netherlands",
            CURRENCY_CODE = "EUR"
          )

          it("should add the 'Netherlands' as COUNTRY to the ContactPersonRecord") {
            val result = ContactPersonConverter.transform(
              spark,
              contactPersonRecord.toDataset,
              countryRecord.toDataset
            ).head()

            result.COUNTRY.get shouldEqual "Netherlands"
          }
        }
      }
    }
  }
}
