package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestContactPersonsGolden
import com.unilever.ohub.spark.export.ddl.model.DdlContactPerson

class ContactPersonDdlConverterSpec extends SparkJobSpec with TestContactPersonsGolden {
  val SUT = ContactPersonDdlConverter
  val contactPersonToConvert = defaultContactPersonGolden.copy(ohubId = Some("12345"))

  describe("Contact person ddl converter") {
    it("should convert a contact person parquet correctly into an contact person csv") {
      val result = SUT.convert(contactPersonToConvert)

      val expectedDdlContactPerson = DdlContactPerson(
        crmConcatId = "",
        contactJobTitle = "Chef",
        otherJobTitle = "",
        decisionMaker = "Y",
        optInSource = "",
        subscriptions = "",
        salutation = "Mr",
        firstName = "John",
        lastName = "Williams",
        phone = "61396621811",
        mobile = "61612345678",
        email = "jwilliams@downunder.au",
        hasDeleted = "",
        afhContactGoldenId = "12345",
        afhCustomerGoldenId = "operator-ohub-id",
        mailingStreet = "Highstreet",
        mailingCity = "Melbourne",
        mailingState = "Alabama",
        mailingPostalCode = "2057",
        mailingCountry = "Australia",
        tps = "",
        contactLanguage = "en",
        optOutDate = "2015-09-30 02:23:02:000",
        optOut = "Y",
        dateAccountAssociatedFrom = "",
        dateAccountAssociatedTo = ""

      )
      result shouldBe expectedDdlContactPerson
    }
  }
}
