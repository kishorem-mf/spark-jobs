package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SparkJobConfig

case class DefaultWithDeltaConfig(
    inputFile: String = "path-to-input-file",
    outputFile: String = "path-to-output-file",
    previousIntegrated: Option[String] = None
) extends SparkJobConfig

case class DefaultWithDeltaConfigParser() extends scopt.OptionParser[DefaultWithDeltaConfig]("ACM converter") {
  head("converts domain entity into ufs entity.", "1.0")
  opt[String]("inputFile") required () action { (x, c) ⇒
    c.copy(inputFile = x)
  } text "inputFile is a string property"
  opt[String]("previousIntegrated") optional () action { (x, c) ⇒
    c.copy(previousIntegrated = Option(x))
  } text "previousIntegrated is a string property"
  opt[String]("outputFile") required () action { (x, c) ⇒
    c.copy(outputFile = x)
  } text "outputFile is a string property"

  version("1.0")
  help("help") text "help text"
}
