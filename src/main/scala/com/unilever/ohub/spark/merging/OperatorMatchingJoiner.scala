package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.Operator

object OperatorMatchingJoiner extends BaseMatchingJoiner[Operator] {

  private[merging] def markGoldenRecordAndGroupId(sourcePreference: Map[String, Int])(entities: Seq[Operator]): Seq[Operator] = {
    val goldenRecord = pickGoldenRecord(sourcePreference, entities)
    val groupId = UUID.randomUUID().toString
    entities.map(o ⇒ o.copy(ohubId = Some(groupId), isGoldenRecord = o == goldenRecord))
  }
}
