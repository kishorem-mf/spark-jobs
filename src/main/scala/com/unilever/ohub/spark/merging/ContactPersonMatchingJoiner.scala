package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.ContactPerson

object ContactPersonMatchingJoiner extends BaseMatchingJoiner[ContactPerson] {

  private[merging] def markGoldenRecordAndGroupId(sourcePreference: Map[String, Int])(entities: Seq[ContactPerson]): Seq[ContactPerson] = {
    val goldenRecord = pickGoldenRecord(sourcePreference, entities)
    val groupId = UUID.randomUUID().toString
    entities.map(o ⇒ o.copy(ohubId = Some(groupId), isGoldenRecord = o == goldenRecord))
  }
}
