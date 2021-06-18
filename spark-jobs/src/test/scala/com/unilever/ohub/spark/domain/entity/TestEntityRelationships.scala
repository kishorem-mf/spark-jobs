package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp


trait TestEntityRelationships {

  lazy val defaultEntityRelationships: EntityRelationships = EntityRelationships(
    id = "id-1",
    creationTimestamp = new Timestamp(1542205922011L),
    countryCode = "DE",
    sourceName = "EMAKINA",
    sourceEntityId = "123",
    concatId = "DE~EMAKINA~123",
    relationshipName = Some("relationshipName"),
    relationshipType = "relationshipType",
    fromEntityType = "fromEntityType",
    fromEntitySourceName = "EMAKINA",
    fromSourceEntityId = "123",
    fromConcatId = "DE~EMAKINA~123",
    fromEntityOhubId = Some("555"),
    fromEntityName = Some("Operator"),
    toEntityType = "toEntityType",
    toEntitySourceName = "EMAKINA",
    toSourceEntityId = "456",
    toConcatId = "DE~EMAKINA~456",
    toEntityOhubId = Some("123445"),
    toEntityName = Some("Operator"),
    isActive = true,
    validFrom = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
    validTo = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
    dateCreated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
    dateUpdated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),

    customerType = "OPERATOR",
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubId = Option.empty,
    isGoldenRecord = false,

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
