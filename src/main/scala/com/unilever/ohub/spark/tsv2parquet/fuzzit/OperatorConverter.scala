package com.unilever.ohub.spark.tsv2parquet.fuzzit

import com.unilever.ohub.spark.domain.Operator
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeper
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeper.ErrorMessage
import org.apache.spark.sql.Row

class OperatorConverter extends DomainGateKeeper[Operator] {

  override final val fieldSeparator: String = ";"

  override final val hasHeaders = false

  override final val partitionByValue = Seq.empty

  override protected def transform: Row => Either[(Row, ErrorMessage), Operator] =
    row =>
      try {
        Right(Operator(
          id = row.getAs[String]("REF_OPERATOR_ID").toLong,               // this one can throw a NumberFormatException
          source = row.getAs[String]("SOURCE"),                           // TODO enforce rules on this field too
          countryCode = row.getAs[String]("COUNTRY_CODE"),                // probably want to enforce some rules one these fields too (mandatory? exactly one? in the list of valid country codes!)
          name = row.getAs[String]("NAME"),
          street = Option(row.getAs[String]("STREET"))
        ))
      } catch {
        case e: Exception => Left((row, s"Error parsing row: '$row', got exception: $e"))
      }
}
