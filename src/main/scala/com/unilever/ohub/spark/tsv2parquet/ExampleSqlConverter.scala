import java.sql.DriverManager
import java.util.Properties

import com.unilever.ohub.spark.tsv2parquet.ExampleRecord
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source

/*
This example expects postgres on localhost with the following database and user:

CREATE DATABASE ufs_example;
CREATE USER ufs_example WITH PASSWORD 'ufs_example';
GRANT ALL PRIVILEGES ON DATABASE ufs_example TO ufs_example;
*/

object ExampleSqlConverter extends App {

  if (args.length != 1) {
    println("specify INPUT_FILE")
    sys.exit(1)
  }

  val inputFile = args(0)

  val jdbcUrl = "jdbc:postgresql://localhost:5432/ufs_example"
  val jdbcTable = "example"
  val jdbcProperties = new Properties
  jdbcProperties.put("user", "ufs_example")
  jdbcProperties.put("password", "ufs_example")

  println("Preparing example database")
  val connection = DriverManager.getConnection(jdbcUrl, jdbcProperties)
  val query = Source.fromInputStream(getClass.getResourceAsStream("/example_table.sql")).mkString
  connection.prepareCall(query).execute()
  connection.close()

  println("Done preparing example database")

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

  println("Contents of TSV file")
  records.show()

  val dbRecords = spark.read.jdbc(jdbcUrl, jdbcTable, jdbcProperties)

  println("Schema of db table")
  dbRecords.printSchema()

  println("Contents of db table before")
  dbRecords.show()

  println("Writing data to db table")
  records.toDF(records.columns map(_.toLowerCase): _*).write.mode(Append).jdbc(jdbcUrl, jdbcTable, jdbcProperties)

  println("Contents of db table after")
  spark.read.jdbc(jdbcUrl, jdbcTable, jdbcProperties).show()

  spark.read.jdbc(jdbcUrl, jdbcTable, jdbcProperties).foreach(println(_))

  println("Done")
}
