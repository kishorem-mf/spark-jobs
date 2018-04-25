package com.unilever.ohub.spark.storage

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession.spark
import org.apache.hadoop.fs.{ FileSystem, Path }

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
      assert(files.length == 27)
      files.foreach(f ⇒ {
        assert(f.toString.endsWith(".csv"))
      })
    }
  }
}
