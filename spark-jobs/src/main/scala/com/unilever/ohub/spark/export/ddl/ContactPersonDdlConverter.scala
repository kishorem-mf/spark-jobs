package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.entity.ContactPersonGolden
import com.unilever.ohub.spark.export._
import com.unilever.ohub.spark.export.ddl.model.DdlContactPerson

object ContactPersonDdlConverter extends Converter[ContactPersonGolden, DdlContactPerson] with TypeConversionFunctions {

  override def convert(implicit cp: ContactPersonGolden, explain: Boolean = false): DdlContactPerson = {
    DdlContactPerson(
      `CRM ContactID` = Option.empty,
      `Contact Job Title` = getValue("jobTitle"),
      `Other Job Title` = Option.empty,
      `Decision Maker` = getValue("isKeyDecisionMaker", BooleanToYNConverter),
      `Opt In Source` = Option.empty,
      Subscriptions = Option.empty,
      Salutation = getValue("title"),
      `First Name` = getValue("firstName"),
      `Last Name` = getValue("lastName"),
      Phone = getValue("phoneNumber"),
      Mobile = getValue("mobileNumber"),
      Email = getValue("emailAddress"),
      `Has Deleted` = Option.empty,
      `AFH Contact Golden ID` = getValue("ohubId"),
      `AFH Customer Golden ID` = getValue("operatorOhubId"),
      MailingStreet = getValue("street"),
      MailingCity = getValue("city"),
      MailingState = getValue("state"),
      MailingPostalCode = getValue("zipCode"),
      MailingCountry = getValue("countryName"),
      TPS = Option.empty,
      `Contact Language` = getValue("language"),
      `Opt out date` = getValue("emailOptInDate"),
      `Opt out` = getValue("hasEmailOptIn", BooleanToYNConverter),
      `Date Account Associated From` = Option.empty,
      `Date Account Associated To` = Option.empty,
      `Email Opt In` = getValue("hasEmailOptIn", BooleanToYNConverter),
      `Email Opt In date` = getValue("emailOptInDate"),
      `Email Opt In 2` = getValue("hasEmailDoubleOptIn", BooleanToYNConverter),
      `Email Opt In 2 date` = getValue("emailDoubleOptInDate"),
      `Email Opt In Status` = Option.empty
    )
  }
}
