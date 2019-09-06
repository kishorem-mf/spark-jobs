package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.ContactPerson

case class PickDatesForContactPerson(cp: ContactPerson) {
  val dateUpdated = cp.dateUpdated.orElse(cp.dateCreated)
  val dateCreated = cp.dateCreated
}
