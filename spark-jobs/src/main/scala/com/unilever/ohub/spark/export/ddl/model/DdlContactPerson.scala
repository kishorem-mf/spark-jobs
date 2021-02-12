package com.unilever.ohub.spark.export.ddl.model

import com.unilever.ohub.spark.export.DDLOutboundEntity

case class DdlContactPerson (
                            crmConcatId: String,
                            contactJobTitle: String,
                            otherJobTitle: String,
                            decisionMaker: String,
                            optInSource: String,
                            subscriptions: String,
                            salutation: String,
                            firstName: String,
                            lastName: String,
                            phone: String,
                            mobile: String,
                            email: String,
                            hasDeleted: String,
                            afhContactGoldenId: String,
                            afhCustomerGoldenId: String,
                            mailingStreet: String,
                            mailingCity: String,
                            mailingState: String,
                            mailingPostalCode: String,
                            mailingCountry: String,
                            tps: String,
                            contactLanguage: String,
                            optOutDate: String,
                            optOut: String,
                            dateAccountAssociatedFrom: String,
                            dateAccountAssociatedTo: String
                            ) extends DDLOutboundEntity
