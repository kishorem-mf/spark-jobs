package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SparkJobConfig

case class DefaultWithDbAndDeltaConfig(
    inputFile: String = "path-to-input-file",
    outputFile: String = "path-to-output-file",
    previousIntegrated: Option[String] = None,
    postgressUrl: String = "postgress-url",
    postgressUsername: String = "postgress-username",
    postgressPassword: String = "postgress-password",
    postgressDB: String = "postgress-db"
) extends SparkJobConfig

case class DefaultWithDbAndDeltaConfigParser() extends scopt.OptionParser[DefaultWithDbAndDeltaConfig]("ACM converter") {
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
  opt[String]("postgressUrl") required () action { (x, c) ⇒
    c.copy(postgressUrl = x)
  } text "postgressUrl is a string property"
  opt[String]("postgressUsername") required () action { (x, c) ⇒
    c.copy(postgressUsername = x)
  } text "postgressUsername is a string property"
  opt[String]("postgressPassword") required () action { (x, c) ⇒
    c.copy(postgressPassword = x)
  } text "postgressPassword is a string property"
  opt[String]("postgressDB") required () action { (x, c) ⇒
    c.copy(postgressDB = x)
  } text "postgressDB is a string property"

  version("1.0")
  help("help") text "help text"
}
