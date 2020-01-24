package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.TestContactPersons
import com.unilever.ohub.spark.export.dispatch.model.DispatchContactPerson
import org.scalatest.{FunSpec, Matchers}

class ContactPersonDispatchConverterTest extends FunSpec with TestContactPersons with Matchers {

  private[dispatch] val SUT = ContactPersonDispatchConverter

  describe("contact person dispatch converter") {
    it("should convert a domain contact person correctly into an dispatch record") {
      val cp = defaultContactPerson.copy(isGoldenRecord = true)
      val actualDispatchContactPerson = SUT.convert(cp)
      print(actualDispatchContactPerson)
      val expectedDispatchContactPerson =
        DispatchContactPerson(
          COUNTRY_CODE = "AU",
          CP_ORIG_INTEGRATION_ID = "AU~WUFOO~AB123",
          CP_LNKD_INTEGRATION_ID = cp.ohubId.get,
          GOLDEN_RECORD_FLAG = "Y",
          SOURCE = "WUFOO",
          SOURCE_ID = "AB123",
          DELETE_FLAG = "N",
          CREATED_AT = "2015-06-30 13:49:00",
          UPDATED_AT = "2015-06-30 13:50:00",
          GENDER = "M",
          ROLE = "Chef",
          TITLE = "Mr",
          FIRST_NAME = "John",
          LAST_NAME = "Williams",
          STREET = "Highstreet",
          HOUSE_NUMBER = "443",
          HOUSE_NUMBER_ADD = "A",
          ZIP_CODE = "2057",
          CITY = "Melbourne",
          COUNTRY = "Australia",
          DM_OPT_OUT = "Y",
          EMAIL_ADDRESS = "jwilliams@downunder.au",
          EMAIL_OPT_OUT =  "Y",
          FIXED_OPT_OUT = "Y",
          FIXED_PHONE_NUMBER = "61396621811",
          MOBILE_OPT_OUT = "Y",
          MOBILE_PHONE_NUMBER = "61612345678",
          LANGUAGE = "en",
          PREFERRED = "Y",
          KEY_DECISION_MAKER = "Y",
          FAX_OPT_OUT = "Y",
          FAX_NUMBER = "61396621812",
          DATE_OF_BIRTH = "1975-12-21",
          SCM = "Mobile",
          STATE = "Alabama",
          OPT_IN = "Y",
          OPT_IN_DATE = "2015-09-30 14:23:02",
          CONFIRMED_OPT_IN = "Y",
          CONFIRMED_OPT_IN_DATE = "2015-09-30 14:23:03",
          OPR_ORIG_INTEGRATION_ID = "AU~WUFOO~E1-1234",
          MOB_OPT_IN = "Y",
          MOB_OPT_IN_DATE = "2015-09-30 14:23:04",
          MOB_CONFIRMED_OPT_IN = "Y",
          MOB_CONFIRMED_OPT_IN_DATE = "2015-09-30 14:23:05"
        )

       actualDispatchContactPerson shouldBe expectedDispatchContactPerson
    }
    it("It should clean e-mail when email is marked as not valid") {
      val cp = defaultContactPerson.copy(isGoldenRecord = true).copy(isEmailAddressValid = Some(false))
      val actualDispatchContactPerson = SUT.convert(cp)

      assert(defaultContactPerson.emailAddress.contains(actualDispatchContactPerson.EMAIL_ADDRESS))
    }
    it("It should NOT clean e-mail when email is marked as valid") {
      val cp = defaultContactPerson.copy(isGoldenRecord = true).copy(isEmailAddressValid = Some(true))
      val actualDispatchContactPerson = SUT.convert(cp)

      assert(actualDispatchContactPerson.EMAIL_ADDRESS contains("jwilliams@downunder.au"))
    }
  }
}
