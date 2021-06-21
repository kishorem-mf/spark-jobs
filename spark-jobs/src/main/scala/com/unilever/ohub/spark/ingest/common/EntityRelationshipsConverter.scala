package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.EntityRelationships
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{DomainTransformer, EntityRelationshipsEmptyParquetWriter}
import org.apache.spark.sql.Row

object EntityType extends Enumeration {
  type EntityType = Value

  val AFFILIATION, AGGREGATOR, BUYINGGROUP, CONTRACTCATERER, OPERATOR, WHOLESALER = Value

  def isEntityType(s: String) = values.exists(_.toString == s) //scalastyle:off
}


object EntityRelationshipsConverter extends CommonDomainGateKeeper[EntityRelationships] with EntityRelationshipsEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ EntityRelationships = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      var ohubCreated = new Timestamp(System.currentTimeMillis())

      EntityRelationships(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        countryCode = mandatory("countryCode"),
        sourceName = mandatory("sourceName"),
        sourceEntityId = mandatory("sourceEntityId"),
        concatId = mandatory("concatId"),
        relationshipName = optional("relationshipName"),
        relationshipType = mandatory("relationshipType"),
        fromEntityType = mandatory("fromEntityType", contains),
        fromEntitySourceName = mandatory("fromEntitySourceName"),
        fromSourceEntityId = mandatory("fromSourceEntityId"),
        fromConcatId = mandatory("fromConcatId"),
        fromEntityOhubId = optional("fromEntityOhubId"),
        fromEntityName = optional("fromEntityName"),
        toEntityType = mandatory("toEntityType", contains),
        toEntitySourceName = mandatory("toEntitySourceName"),
        toSourceEntityId = mandatory("toSourceEntityId"),
        toConcatId = mandatory("toConcatId"),
        toEntityOhubId = optional("toEntityOhubId"),
        toEntityName = optional("toEntityName"),
        isActive = mandatory("isActive", toBoolean),
        validFrom = optional("validFrom", parseDateTimeUnsafe()),
        validTo = optional("validTo", parseDateTimeUnsafe()),
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),

        customerType = mandatory("customerType"),
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,
        ohubId = Option.empty,
        isGoldenRecord = false,
        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
