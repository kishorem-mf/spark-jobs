package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.export._
import com.unilever.ohub.spark.export.ddl.model.DdlContactPerson

object ContactPersonDdlConverter extends Converter[ContactPerson, DdlContactPerson] with TypeConversionFunctions {

  override def convert(implicit cp: ContactPerson, explain: Boolean = false): DdlContactPerson = {
    DdlContactPerson(
      crmConcatId = Option.empty,
      contactJobTitle = getValue("jobTitle"),
      otherJobTitle = Option.empty,
      decisionMaker = getValue("isKeyDecisionMaker", BooleanToYNConverter),
      optInSource = Option.empty,
      subscriptions = Option.empty,
      salutation = getValue("title"),
      firstName = getValue("firstName"),
      lastName = getValue("lastName"),
      phone = getValue("phoneNumber"),
      mobile = getValue("mobileNumber"),
      email = getValue("emailAddress"),
      hasDeleted = Option.empty,
      afhContactGoldenId = getValue("ohubId"),
      afhCustomerGoldenId = getValue("operatorOhubId"),
      mailingStreet = getValue("street"),
      mailingCity = getValue("city"),
      mailingState = getValue("state"),
      mailingPostalCode = getValue("zipCode"),
      mailingCountry = getValue("countryName"),
      tps = Option.empty,
      contactLanguage = getValue("language"),
      optOutDate = getValue("emailOptInDate"),
      optOut = getValue("hasEmailOptIn", BooleanToYNConverter),
      dateAccountAssociatedFrom = Option.empty,
      dateAccountAssociatedTo = Option.empty
    )
  }
}
