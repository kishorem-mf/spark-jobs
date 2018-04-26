package com.unilever.ohub.spark.storage

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession.spark
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.functions._

class DefaultStorageSpec extends SparkJobSpec {

  val victim = new DefaultStorage(spark)
  val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  describe("isConcatAvailable") {
    it("should return false on local filesystem") {
      assert(!victim.isConcatAvailable(fs))
    }
  }

  describe("getCsvFilePaths") {
    it("should return only and all csvs files in a path") {
      val files = victim.getCsvFilePaths(fs, new Path("src/test/"))
      assert(files.length >= 27)
      files.foreach(f ⇒ {
        assert(f.toString.endsWith(".csv"))
      })
    }
  }

  describe("writeToSingleCsv") {
    it("should write a single csv file if concat is not available") {
      val ds = spark
        .range(1, 1000, 1, 4)
        .withColumn("random", (rand() * 10).cast("int"))

      val fileName = "src/test/resources/test_output/single.csv"
      victim.writeToSingleCsv(ds, fileName)(null)
      val file = new java.io.File(fileName)
      assert(file.exists)
      assert(file.isFile)
    }
  }
}
