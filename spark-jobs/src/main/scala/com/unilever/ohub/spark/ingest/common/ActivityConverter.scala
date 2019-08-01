package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Activity
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{ActivityEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object ActivityConverter extends CommonDomainGateKeeper[Activity] with ActivityEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Activity = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      Activity(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = mandatory("customerType"),
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        isActive = mandatory("isActive", toBoolean),
        isGoldenRecord = false,
        sourceEntityId = mandatory("sourceEntityId"),
        sourceName = mandatory("sourceName"),
        ohubId = Option.empty,
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,

        activityDate = optional("activityDate", parseDateTimeUnsafe()),
        name = optional("name"),
        details = optional("details"),
        actionType = optional("actionType"),
        contactPersonConcatId = optional("contactPersonConcatId"),
        contactPersonOhubId = Option.empty,
        operatorConcatId = optional("operatorConcatId"),
        operatorOhubId = Option.empty,
        activityId = optional("activityId"),

        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
