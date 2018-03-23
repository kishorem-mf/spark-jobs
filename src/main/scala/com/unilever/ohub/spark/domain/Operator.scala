package com.unilever.ohub.spark.domain


// TODO not the actual domain entity (this needs to be specified, see OHUB-215)
case class Operator(
                     id: Long,
                     source: String,
                     countryCode: String,
                     name : String,
                     street: Option[String]
                   )
