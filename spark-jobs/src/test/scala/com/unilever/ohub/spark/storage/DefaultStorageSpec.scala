package com.unilever.ohub.spark.storage

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row

class DefaultStorageSpec extends SparkJobSpec {

  val victim = new DefaultStorage(spark)
  val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  describe("getCsvFilePaths") {
    it("should return only and all csvs files in a path") {
      val files = victim.getCsvFilePaths(fs, new Path("src/test/"))
      assert(files.length >= 10)
      files.foreach(f ⇒ {
        assert(f.toString.endsWith(".csv"))
      })
    }
  }

  describe("readFromCsv") {
    it("should read a csv with quotes correctly") {
      val result = victim.readFromCsv("src/test/resources/input/quoted.csv", ";")

      result.select("id", "name").collect() shouldBe Array(
        Row("1", "\"Mi Casa\""),
        Row("2", "Mi; Casa"),
        Row("3", "\"Mi; Casa\""),
        Row("4", "\"\"Mi; Casa\"\"")
      )
    }
  }
}
