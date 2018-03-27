package com.unilever.ohub.spark.tsv2parquet.fuzzit

import com.unilever.ohub.spark.domain.Operator
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeper
import org.apache.spark.sql.Row

object OperatorConverter extends DomainGateKeeper[Operator] {

  override final val fieldSeparator: String = ";"

  override final val hasHeaders = true

  override final val partitionByValue = Seq.empty

  override val toDomainEntity: Row => Operator =
    row =>
      Operator(
        id = row.getAs[String](0).toLong, // this one can throw a NumberFormatException
        source = row.getAs[String](1), // TODO enforce rules on this field too
        countryCode = row.getAs[String](2), // probably want to enforce some rules one these fields too (mandatory? exactly one? in the list of valid country codes!)
        name = row.getAs[String](3),
        street = Option(row.getAs[String](4))
      )
}
