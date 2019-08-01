package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Answer
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{AnswerEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object AnswerConverter extends CommonDomainGateKeeper[Answer] with AnswerEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Answer = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      Answer(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = mandatory("customerType"),
        isActive = mandatory("isActive", toBoolean),
        sourceEntityId = mandatory("sourceEntityId"),
        sourceName = mandatory("sourceName"),
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        ohubId = Option.empty,
        isGoldenRecord = false,

        answer = optional("answer"),
        questionConcatId = mandatory("questionConcatId"),

        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
