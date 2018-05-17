package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.ContactPerson

object ContactPersonMatchingJoiner extends BaseMatchingJoiner[ContactPerson] {

  override private[merging] def markGoldenRecordAndGroup(entity: ContactPerson, goldenRecord: ContactPerson, groupId: String): ContactPerson =
    entity.copy(ohubId = Some(groupId), isGoldenRecord = entity == goldenRecord)
}
