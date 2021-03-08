package com.unilever.ohub.spark.export.ddl.model

import com.unilever.ohub.spark.export.DDLOutboundEntity

case class DdlNewsletterSubscription(
                                      `Newsletter Number`: String,
                                      ID: String,
                                      `Contact SAP ID`: String,
                                      `Created By`: String,
                                      Currency: String,
                                      `Delivery Method`: String,
                                      `Include Pricing`: String,
                                      Language: String,
                                      `Newsletter ID`: String,
                                      `Newsletter Name`: String,
                                      Owner: String,
                                      Quantity: String,
                                      `Record Type`: String,
                                      Status: String,
                                      `Status Opt Out`: String,
                                      Subscribed: String,
                                      `Subscription Confirmed`: String,
                                      `Subscription Confirmed Date`: String,
                                      `Subscription Date`: String,
                                      `Subscription Type`: String,
                                      `Unsubscription Confirmed Date`: String,
                                      `Unsubscription Date`: String,
                                      Comment: String
                                    ) extends DDLOutboundEntity
