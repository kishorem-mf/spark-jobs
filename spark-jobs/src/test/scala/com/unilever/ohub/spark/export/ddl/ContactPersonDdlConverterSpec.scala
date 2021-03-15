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
        `CRM ContactID`  = "",
        `Contact Job Title` = "Chef",
        `Other Job Title` = "",
        `Decision Maker` = "Y",
        `Opt In Source` = "",
        Subscriptions = "",
        Salutation = "Mr",
        `First Name` = "John",
        `Last Name` = "Williams",
        Phone = "61396621811",
        Mobile = "61612345678",
        Email = "jwilliams@downunder.au",
        `Has Deleted` = "",
        `AFH Contact Golden ID` = "12345",
        `AFH Customer Golden ID` = "operator-ohub-id",
        MailingStreet = "Highstreet",
        MailingCity = "Melbourne",
        MailingState = "Alabama",
        MailingPostalCode = "2057",
        MailingCountry = "Australia",
        TPS = "",
        `Contact Language` = "en",
        `Opt out date` = "2015-09-30 02:23:02:000",
        `Opt out` = "Y",
        `Date Account Associated From` = "",
        `Date Account Associated To` = ""
      )
      result shouldBe expectedDdlContactPerson
    }
  }
}
