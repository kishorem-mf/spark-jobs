package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity

trait GoldenRecordPicking[DomainType <: DomainEntity] {
  val lowDate = new Timestamp(0)

  def pickGoldenRecord(sourcePreference: Map[String, Int], entities: Seq[DomainType]): DomainType = {
    entities.reduce((o1, o2) ⇒ {
      val preference1 = sourcePreference.getOrElse(o1.sourceName, Int.MaxValue)
      val preference2 = sourcePreference.getOrElse(o2.sourceName, Int.MaxValue)
      if (preference1 < preference2) o1
      else if (preference1 > preference2) o2
      else { // same source preference
        var date1 = new Timestamp(0)
        var date2 = new Timestamp(0)

        if (o1.dateUpdated.isEmpty && o2.dateUpdated.isEmpty) {
          date1 = o1.dateCreated.getOrElse(lowDate)
          date2 = o2.dateCreated.getOrElse(lowDate)
        } else if (!o1.dateUpdated.isEmpty && o2.dateUpdated.isEmpty) {
          date1 = o1.dateUpdated.getOrElse(lowDate)
          date2 = o2.dateCreated.getOrElse(lowDate)
        } else if (o1.dateUpdated.isEmpty && !o2.dateUpdated.isEmpty) {
          date1 = o1.dateCreated.getOrElse(lowDate)
          date2 = o2.dateUpdated.getOrElse(lowDate)
        } else if ((!o1.dateUpdated.isEmpty && !o2.dateUpdated.isEmpty) && o1.dateUpdated.equals(o2.dateUpdated)) {
          date1 = o1.dateCreated.getOrElse(lowDate)
          date2 = o2.dateCreated.getOrElse(lowDate)
        } else if ((!o1.dateUpdated.isEmpty && !o2.dateUpdated.isEmpty) && !o1.dateUpdated.equals(o2.dateUpdated)) {
          date1 = o1.dateUpdated.getOrElse(lowDate)
          date2 = o2.dateUpdated.getOrElse(lowDate)
        }

        if (date1.equals(date2) && o1.isGoldenRecord) o1 // If it already was golden and dates are the same: prefer that one
        else if (date1.after(date2)) o1
        else o2
      }
    })
  }
}
