package com.unilever.ohub.spark.export.ddl.model

import com.unilever.ohub.spark.export.DDLOutboundEntity

case class DdlNewsletterSubscription(
                                      newsletterNumber: String,
                                      id: String,
                                      contactSAPID: String,
                                      createdBy: String,
                                      currency: String,
                                      deliveryMethod: String,
                                      includePricing: String,
                                      language: String,
                                      newsletterId: String,
                                      newsletterName: String,
                                      owner: String,
                                      quantity: String,
                                      recordType: String,
                                      status: String,
                                      statusOptOut: String,
                                      subscribed: String,
                                      subscriptionConfirmed: String,
                                      subscriptionConfirmedDate: String,
                                      subscriptionDate: String,
                                      subscriptionType: String,
                                      unsubscriptionConfirmedDate: String,
                                      unsubscriptionDate: String,
                                      comment: String
                                    ) extends DDLOutboundEntity
