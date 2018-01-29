package com.unilever.ohub.spark.merging

import org.apache.spark.sql.{Dataset, SparkSession}


// The step that fixes the foreign key links between contact persons and operators
// Temporarily in a 2nd file to make development easier, will end up in the first OperatorMerging job eventually.
object ContactPersonMerging2 extends App {

  if (args.length != 3) {
    println("specify CONTACT_PERSON_MATCHING_INPUT_FILE OPERATOR_MATCHING_INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val contactPersonMatchingInputFile = args(0)
  val operatorMatchingInputFile = args(1)
  val outputFile = args(2)

  println(s"Merging contact persons from [$contactPersonMatchingInputFile] and [$operatorMatchingInputFile] to [$outputFile]")

  val spark = SparkSession.
    builder().
    appName(this.getClass.getSimpleName).
    getOrCreate()

  import spark.implicits._

  val startOfJob = System.currentTimeMillis()

  val contactPersonMatching: Dataset[GoldenContactPersonRecord] = spark.read.parquet(contactPersonMatchingInputFile)
    .as[GoldenContactPersonRecord]

  println("contactPersonMatching " + contactPersonMatching.count())

  val operatorMatching: Dataset[GoldenOperatorRecord] = spark.read.parquet(operatorMatchingInputFile)
    .as[GoldenOperatorRecord]

  println("operatorMatching " + operatorMatching.count())
}
