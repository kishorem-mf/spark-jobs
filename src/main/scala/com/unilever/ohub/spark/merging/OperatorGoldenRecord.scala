package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Operator

trait OperatorGoldenRecord {

  def pickGoldenRecord(sourcePreference: Map[String, Int], operators: Seq[Operator]): Operator = {
    operators.reduce((o1, o2) ⇒ {
      val preference1 = sourcePreference.getOrElse(o1.sourceName, Int.MaxValue)
      val preference2 = sourcePreference.getOrElse(o2.sourceName, Int.MaxValue)
      if (preference1 < preference2) o1
      else if (preference1 > preference2) o2
      else { // same source preference
        val created1 = o1.dateCreated
        val created2 = o2.dateCreated
        if (created1.after(created2)) o1 else o2
      }
    })
  }

}
