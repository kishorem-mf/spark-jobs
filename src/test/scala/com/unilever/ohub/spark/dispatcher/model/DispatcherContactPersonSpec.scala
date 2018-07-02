package com.unilever.ohub.spark.dispatcher
package model

import com.unilever.ohub.spark.SimpleSpec
import com.unilever.ohub.spark.domain.entity.TestContactPersons

class DispatcherContactPersonSpec extends SimpleSpec {

  final val TEST_CONTACT_PERSON = {
    TestContactPersons.defaultContactPerson
      .copy(isGoldenRecord = true)
      .copy(ohubId = Some("randomId"))
  }

  describe("DispatcherContactPerson") {
    it("should map from a ContactPerson") {
      DispatcherContactPerson.fromContactPerson(TEST_CONTACT_PERSON) shouldEqual DispatcherContactPerson(
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
        MIDDLE_NAME = Option.empty,
        ROLE = Some("Chef"),
        ORG_FIRST_NAME = Option.empty,
        ORG_LAST_NAME = Option.empty,
        ORG_EMAIL_ADDRESS = Option.empty,
        ORG_FIXED_PHONE_NUMBER = Option.empty,
        ORG_MOBILE_PHONE_NUMBER = Option.empty,
        ORG_FAX_NUMBER = Option.empty,
        MOB_OPT_OUT_DATE = Option.empty
      )
    }
  }
}
