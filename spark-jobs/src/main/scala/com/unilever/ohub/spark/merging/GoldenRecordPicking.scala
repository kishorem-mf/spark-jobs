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
        var dateCreatedOrUpdated1 = new Timestamp(0)
        var dateCreatedOrUpdated2 = new Timestamp(0)

        val isO2DateUpdatedEmpty = o2.dateUpdated.isEmpty

        (o1.dateUpdated.isEmpty,isO2DateUpdatedEmpty) match {
          case (true,true) => dateCreatedOrUpdated1 = o1.dateCreated.getOrElse(lowDate)
            dateCreatedOrUpdated2 = o2.dateCreated.getOrElse(lowDate)
          case (true, false) => dateCreatedOrUpdated1 = o1.dateCreated.getOrElse(lowDate)
            dateCreatedOrUpdated2 = o2.dateUpdated.getOrElse(lowDate)
          case (false,_) => isO2DateUpdatedEmpty match {
            case true =>dateCreatedOrUpdated1 = o1.dateUpdated.getOrElse(lowDate)
              dateCreatedOrUpdated2 = o2.dateCreated.getOrElse(lowDate)
            case false  => o1.dateUpdated.equals(o2.dateUpdated) match {
              case true =>dateCreatedOrUpdated1 = o1.dateCreated.getOrElse(lowDate)
                dateCreatedOrUpdated2 = o2.dateCreated.getOrElse(lowDate)
              case false =>dateCreatedOrUpdated1 = o1.dateUpdated.getOrElse(lowDate)
                dateCreatedOrUpdated2 = o2.dateUpdated.getOrElse(lowDate)
            }
          }
        }

        if (dateCreatedOrUpdated1.equals(dateCreatedOrUpdated2) && o1.isGoldenRecord) o1 // If it already was golden and dates are the same: prefer that one
        else if (dateCreatedOrUpdated1.after(dateCreatedOrUpdated2)) o1
        else o2
      }
    })
  }
}
