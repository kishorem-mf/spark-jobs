package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.Question
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{ DomainTransformer, QuestionEmptyParquetWriter }
import org.apache.spark.sql.Row

object QuestionConverter extends CommonDomainGateKeeper[Question] with QuestionEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Question = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val ohubCreated = currentTimestamp()

    // format: OFF

    Question(
      // fieldName                  mandatory                   sourceFieldName             targetFieldName                 transformationFunction (unsafe)
      id                          = mandatory(                  "id",                         "id"),
      creationTimestamp           = mandatory(                  "creationTimestamp",          "creationTimestamp", toTimestamp),
      concatId                    = mandatory(                  "concatId",                   "concatId"),
      countryCode                 = mandatory(                  "countryCode",                "countryCode"),
      customerType                = mandatory(                  "customerType",               "customerType"),
      dateCreated                 = optional(                   "dateCreated",                "dateCreated", parseDateTimeUnsafe()),
      dateUpdated                 = optional(                   "dateUpdated",                "dateUpdated", parseDateTimeUnsafe()),
      isActive                    = mandatory(                  "isActive",                   "isActive", toBoolean ),
      isGoldenRecord              = false,
      sourceEntityId              = mandatory(                  "sourceEntityId",             "sourceEntityId"),
      sourceName                  = mandatory(                  "sourceName",                 "sourceName"),
      ohubId                      = Option.empty,
      ohubCreated                 = ohubCreated,
      ohubUpdated                 = ohubCreated,

      activityConcatId            = mandatory(                  "activityConcatId",       "activityConcatId"),
      question                    = optional(                   "questionType",           "questionType"),

      additionalFields            = additionalFields,
      ingestionErrors             = errors
    )

    // format: ON
  }
}
