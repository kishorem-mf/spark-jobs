package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.ContactPerson

case class PickDatesForContactPerson(
                                      dateUpdated: Option[Timestamp],
                                      dateCreated: Option[Timestamp],
                                      cp: ContactPerson)
