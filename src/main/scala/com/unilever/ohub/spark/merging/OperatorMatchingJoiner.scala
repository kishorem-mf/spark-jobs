package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.Operator

object OperatorMatchingJoiner extends BaseMatchingJoiner[Operator] {

  override private[merging] def markGoldenRecordAndGroup(entity: Operator, goldenRecord: Operator, groupId: String): Operator =
    entity.copy(ohubId = Some(groupId), isGoldenRecord = entity == goldenRecord)
}
