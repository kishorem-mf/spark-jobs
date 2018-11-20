package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.Activity
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{ DomainTransformer, ActivityEmptyParquetWriter }
import org.apache.spark.sql.Row
import java.util.UUID

object ActivityConverter extends CommonDomainGateKeeper[Activity] with ActivityEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Activity = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val ohubCreated = currentTimestamp()

    // format: OFF

    Activity(
      // fieldName                  mandatory                   sourceFieldName             targetFieldName                 transformationFunction (unsafe)
      id                          = mandatory(                  "id",                         "id"),
      creationTimestamp           = mandatory(                  "creationTimestamp",          "creationTimestamp", toTimestamp),
      concatId                    = mandatory(                  "concatId",                   "concatId"),
      countryCode                 = mandatory(                  "countryCode",                "countryCode"),
      customerType                = mandatory(                  "customerType",               "customerType"),
      dateCreated                 = optional(                   "dateCreated",                "dateCreated", parseDateTimeUnsafe()),
      dateUpdated                 = optional(                   "dateUpdated",                "dateUpdated", parseDateTimeUnsafe()),
      isActive                    = mandatory(                  "isActive",                   "isActive", toBoolean),
      isGoldenRecord              = false,
      sourceEntityId              = mandatory(                  "sourceEntityId",             "sourceEntityId"),
      sourceName                  = mandatory(                  "sourceName",                 "sourceName"),
      ohubId                      = Option.empty,
      ohubCreated                 = ohubCreated,
      ohubUpdated                 = ohubCreated,

      activityDate                = optional(                   "activityDate",               "activityDate", parseDateTimeUnsafe()),
      name                        = optional(                   "name",                       "name"),
      details                     = optional(                   "details",                    "details"),
      actionType                  = optional(                   "actionType",                 "actionType"),
      contactPersonConcatId       = optional(                   "contactPersonConcatId",      "contactPersonConcatId"),
      contactPersonOhubId         = Option.empty,
      operatorConcatId            = optional(                   "operatorConcatId",      "operatorConcatId"),
      operatorOhubId              = Option.empty,

      additionalFields            = additionalFields,
      ingestionErrors             = errors
    )

    // format: ON
  }
}
