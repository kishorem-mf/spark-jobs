package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.TestContactPersons
import com.unilever.ohub.spark.export.acm.model.AcmContactPerson
import org.scalatest.{FunSpec, Matchers}

  class ContactPersonAcmConverterSpec extends FunSpec with TestContactPersons with Matchers {

    private[acm] val SUT = ContactPersonAcmConverter

    describe("contact person acm converter") {
      it("should convert a domain contact person correctly into an acm record") {
        val cp = defaultContactPerson.copy(isGoldenRecord = true)
        val actualAcmContactPerson = SUT.convert(cp)
        val expectedAcmContactPerson =
          AcmContactPerson(
            CP_ORIG_INTEGRATION_ID = defaultContactPerson.ohubId.get,
            CP_LNKD_INTEGRATION_ID = "AU~AB123~3~19",
            OPR_ORIG_INTEGRATION_ID = "operator-ohub-id",
            GOLDEN_RECORD_FLAG = "Y",
            WEB_CONTACT_ID = "",
            EMAIL_OPTOUT = "Y",
            PHONE_OPTOUT = "Y",
            FAX_OPTOUT = "Y",
            MOBILE_OPTOUT = "Y",
            DM_OPTOUT = "Y",
            LAST_NAME = "Williams",
            FIRST_NAME = "John",
            TITLE = "Mr",
            GENDER = "1",
            LANGUAGE = "en",
            EMAIL_ADDRESS = "jwilliams@downunder.au",
            MOBILE_PHONE_NUMBER = "61612345678",
            PHONE_NUMBER = "61396621811",
            FAX_NUMBER = "61396621812",
            STREET = "Highstreet",
            HOUSENUMBER = "443",
            ZIPCODE = "2057",
            CITY = "Melbourne",
            COUNTRY = "Australia",
            DATE_CREATED = "2015/06/30 13:47:00",
            DATE_UPDATED = "2015/06/30 13:48:00",
            DATE_OF_BIRTH = "1975/12/21",
            PREFERRED = "Y",
            ROLE = "Chef",
            COUNTRY_CODE = "AU",
            SCM = "Mobile",
            DELETE_FLAG = "0",
            KEY_DECISION_MAKER = "Y",
            OPT_IN = "Y",
            OPT_IN_DATE = "2015/09/30 14:23:02",
            CONFIRMED_OPT_IN = "Y",
            CONFIRMED_OPT_IN_DATE = "2015/09/30 14:23:03",
            MOB_OPT_IN = "Y",
            MOB_OPT_IN_DATE = "2015/09/30 14:23:04",
            MOB_CONFIRMED_OPT_IN = "Y",
            MOB_CONFIRMED_OPT_IN_DATE = "2015/09/30 14:23:05",
            MOB_OPT_OUT_DATE = "",
            ORG_FIRST_NAME = "John",
            ORG_LAST_NAME = "Williams",
            ORG_EMAIL_ADDRESS = "jwilliams@downunder.au",
            ORG_FIXED_PHONE_NUMBER = "61396621811",
            ORG_MOBILE_PHONE_NUMBER = "61612345678",
            ORG_FAX_NUMBER = "61396621812",
            HAS_REGISTRATION = "Y",
            REGISTRATION_DATE = "2015/09/30 14:23:01",
            HAS_CONFIRMED_REGISTRATION = "Y",
            CONFIRMED_REGISTRATION_DATE = "2015/09/30 14:23:01",
            SOURCE_IDS = "-19-",
            TARGET_OHUB_ID = "")

        actualAcmContactPerson shouldBe expectedAcmContactPerson
      }

      it("It should NOT clean e-mail when email is marked as valid") {
        val cp = defaultContactPerson.copy(isGoldenRecord = true).copy(isEmailAddressValid = Some(true))
        val actualDispatchContactPerson = SUT.convert(cp)

        assert(actualDispatchContactPerson.ORG_EMAIL_ADDRESS contains("jwilliams@downunder.au"))
        assert(actualDispatchContactPerson.EMAIL_ADDRESS contains("jwilliams@downunder.au"))
      }

      it("It should clean mobile when mobileNumber is marked as inValid") {
        val cp = defaultContactPerson.copy(isGoldenRecord = true).copy(isMobileNumberValid = Some(false))
        val actualDispatchContactPerson = SUT.convert(cp)

        assert(actualDispatchContactPerson.ORG_MOBILE_PHONE_NUMBER contains("61612345678"))
        assert(actualDispatchContactPerson.MOBILE_PHONE_NUMBER contains(""))
      }

      it("It should convert GENDER to 0 when empty") {
        val cp = defaultContactPerson.copy(isGoldenRecord = true).copy(gender = None)
        val actualDispatchContactPerson = SUT.convert(cp)

        assert(actualDispatchContactPerson.GENDER equals ("0"))
      }

      it("It should convert sourcenames mapping when all of the sources are available") {
        val cp = defaultContactPerson.copy(isGoldenRecord = true).copy(sourceName="FACEBOOK,GAZPACHO")
        val actualDispatchContactPerson = SUT.convert(cp)

        assert(actualDispatchContactPerson.SOURCE_IDS equals "-51-54-")
      }
      it("It should convert sourcenames mapping when some of the sources aren't available") {
        val cp = defaultContactPerson.copy(isGoldenRecord = true).copy(sourceName="FACEBOOK,NON-EXISTING-SOURCE")
        val actualDispatchContactPerson = SUT.convert(cp)

        assert(actualDispatchContactPerson.SOURCE_IDS equals "-51-")
      }
      it("It should concatenate sourcenames mapping correctly when all of the sources are available") {
        val cp = defaultContactPerson.copy(isGoldenRecord = true).copy(sourceName="NON-EXISTING,ANOTHER-NON-EXISISTING")
        val actualDispatchContactPerson = SUT.convert(cp)

        assert(actualDispatchContactPerson.SOURCE_IDS equals "")
      }

      it("TargetOhubId is not empty when additionalField is set") {
        val cp = defaultContactPerson.copy(isGoldenRecord = true, additionalFields = Map("targetOhubId" -> "AC234"))
        val actualAcmContactPerson = SUT.convert(cp)

        assert(actualAcmContactPerson.TARGET_OHUB_ID equals ("AC234"))
      }

    }
  }
