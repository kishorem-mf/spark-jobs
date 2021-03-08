package com.unilever.ohub.spark.export.ddl.model

import com.unilever.ohub.spark.export.DDLOutboundEntity

case class DdlContactPerson(
                             `CRM ContactID`: String,
                             `Contact Job Title`: String,
                             `Other Job Title`: String,
                             `Decision Maker`: String,
                             `Opt In Source`: String,
                             Subscriptions: String,
                             Salutation: String,
                             `First Name`: String,
                             `Last Name`: String,
                             Phone: String,
                             Mobile: String,
                             Email: String,
                             `Has Deleted`: String,
                             `AFH Contact Golden ID`: String,
                             `AFH Customer Golden ID`: String,
                             MailingStreet: String,
                             MailingCity: String,
                             MailingState: String,
                             MailingPostalCode: String,
                             MailingCountry: String,
                             TPS: String,
                             `Contact Language`: String,
                             `Opt out date`: String,
                             `Opt out`: String,
                             `Date Account Associated From`: String,
                             `Date Account Associated To`: String
                           ) extends DDLOutboundEntity
