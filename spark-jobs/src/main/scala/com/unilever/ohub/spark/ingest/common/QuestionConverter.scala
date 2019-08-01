package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Question
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{DomainTransformer, QuestionEmptyParquetWriter}
import org.apache.spark.sql.Row

object QuestionConverter extends CommonDomainGateKeeper[Question] with QuestionEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Question = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      Question(
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
        activityConcatId = mandatory("activityConcatId"),
        question = optional("question"),
        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
