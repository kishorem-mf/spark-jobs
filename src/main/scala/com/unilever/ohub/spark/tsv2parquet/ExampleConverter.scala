package com.unilever.ohub.spark.tsv2parquet

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.SaveMode._

case class ExampleRecord(firstName: String, lastName:String, number:Int, country:String)

object ExampleConverter extends App {
  if (args.length != 2) {
    println("specify INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val inputFile = args(0)
  val outputFile = args(1)

  println(s"Generating parquet from [$inputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  import spark.implicits._

  val lines: Dataset[String] = spark.read.textFile(inputFile)

  val records:Dataset[ExampleRecord] = lines
    .filter(line => !line.isEmpty && !line.startsWith("firstname"))
    .map(line => line.split('\t'))
    .map(lineParts => {
      new ExampleRecord(
        lineParts(0),
        lineParts(1),
        lineParts(2).toInt,
        lineParts(3)
      )
    })

  records.write.mode(Overwrite).partitionBy("country").format("parquet").save(outputFile)

  records.printSchema()

  // read the parquet file from disk as case classes and print them to check if all went well
  spark.read.parquet(outputFile)
    .as[ExampleRecord]
    .foreach(println(_))

  println("Done")

}
