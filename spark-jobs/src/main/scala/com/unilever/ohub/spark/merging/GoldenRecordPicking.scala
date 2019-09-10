package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity

trait GoldenRecordPicking[DomainType <: DomainEntity] {
  val lowDate = new Timestamp(0)

  def pickGoldenRecord(sourcePreference: Map[String, Int], entities: Seq[DomainType]): DomainType = {
    entities.reduce((o1, o2) ⇒ {
      val preference1 = sourcePreference.getOrElse(o1.sourceName, Int.MaxValue)
      val preference2 = sourcePreference.getOrElse(o2.sourceName, Int.MaxValue)
      if (preference1 < preference2) o1 else if (preference1 > preference2) { o2 }
      else { // same source preference
        val o1Date = o1.dateUpdated.orElse(o1.dateCreated).getOrElse(new Timestamp(0))
        val o2Date = o2.dateUpdated.orElse(o2.dateCreated).getOrElse(new Timestamp(0))

        if (o1Date.equals(o2Date) && o1.isGoldenRecord || o1Date.after(o2Date)) o1 else o2 // If it already was golden and dates are the same: prefer that one
      }
    })
  }
}
