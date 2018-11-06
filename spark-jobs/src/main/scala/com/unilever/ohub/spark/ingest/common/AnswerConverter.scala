package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.Answer
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{ DomainTransformer, AnswerEmptyParquetWriter }
import org.apache.spark.sql.Row
import java.util.UUID

object AnswerConverter extends CommonDomainGateKeeper[Answer] with AnswerEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Answer = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val ohubCreated = currentTimestamp()

    // format: OFF

    Answer(
      // fieldName                  mandatory                   sourceFieldName             targetFieldName                 transformationFunction (unsafe)
      concatId                    = mandatory(                  "concatId",                   "concatId"),
      countryCode                 = mandatory(                  "countryCode",                "countryCode"),
      customerType                = mandatory(                  "customerType",               "customerType"),
      isActive                    = mandatory(                  "isActive",                   "isActive", toBoolean),
      sourceEntityId              = mandatory(                  "sourceEntityId",             "sourceEntityId"),
      sourceName                  = mandatory(                  "sourceName",                 "sourceName"),
      ohubCreated                 = ohubCreated,
      ohubUpdated                 = ohubCreated,
      dateCreated                 = optional(                   "dateCreated",                "dateCreated", parseDateTimeUnsafe()),
      dateUpdated                 = optional(                   "dateUpdated",                "dateUpdated", parseDateTimeUnsafe()),
      ohubId                      = Option.empty,
      isGoldenRecord              = false,

      answer                      = optional(                   "answer",                     "answer"),
      questionConcatId            = mandatory(                  "questionConcatId",           "questionConcatId"),

      additionalFields            = additionalFields,
      ingestionErrors             = errors
    )

    // format: ON
  }
}
