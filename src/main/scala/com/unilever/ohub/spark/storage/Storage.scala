package com.unilever.ohub.spark.storage

import java.util.UUID

import org.apache.spark.sql._
import org.apache.hadoop.fs._
import org.apache.log4j.Logger

trait Storage {

  def readFromCsv(
    location: String,
    fieldSeparator: String,
    hasHeaders: Boolean = true
  ): Dataset[Row]

  def writeToSingleCsv(
    ds: Dataset[_],
    outputFile: String,
    delim: String = ";",
    quote: String = "\""
  )(implicit log: Logger): Unit

  def readFromParquet[T: Encoder](location: String, selectColumns: Seq[Column] = Seq()): Dataset[T]

  def writeToParquet(ds: Dataset[_], location: String, partitionBy: Seq[String] = Seq()): Unit
}

class DefaultStorage(spark: SparkSession) extends Storage {

  override def readFromCsv(
    location: String,
    fieldSeparator: String,
    hasHeaders: Boolean = true
  ): Dataset[Row] = {
    spark
      .read
      .option("header", hasHeaders)
      .option("sep", fieldSeparator)
      .option("inferSchema", value = false)
      .csv(location)
  }

  def writeToSingleCsv(
    ds: Dataset[_],
    outputFile: String,
    delim: String = ";",
    quote: String = "\""
  )(implicit log: Logger): Unit = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val outputFilePath = new Path(outputFile)
    val temporaryPath = new Path(outputFilePath.getParent, UUID.randomUUID().toString)

    val concatAvailable = isConcatAvailable(fs)

    val updatedDs = if (concatAvailable) ds else ds.coalesce(1)
    updatedDs
      .write
      .mode(SaveMode.Overwrite)
      .option("encoding", "UTF-8")
      .option("header", "true")
      .option("quoteAll", "true")
      .option("delimiter", delim)
      .option("quote", quote)
      .csv(temporaryPath.toString)

    createSingleFileFromPath(fs, outputFilePath, temporaryPath, concatAvailable, getCsvFilePaths)
  }

  private[storage] def createSingleFileFromPath(
    fs: FileSystem,
    outputFilePath: Path,
    inputPath: Path,
    concatAvailable: Boolean,
    getFilesFun: (FileSystem, Path) ⇒ Array[Path]
  )(implicit log: Logger) = {
    val paths = getFilesFun(fs, inputPath)
      fs.delete(inputPath, true)

    if (concatAvailable) {
      fs.concat(outputFilePath, paths)
      fs.delete(inputPath, true)
    } else {
      if (paths.length != 1) {
        log.error("the number of files found is greater or smaller than 1, this is not supported")
        System.exit(1)
      }
      fs.rename(paths.head, outputFilePath)
      fs.delete(inputPath, true)
    }
  }

  private[storage] def isConcatAvailable(fs: FileSystem) = {
    try {
      fs.concat(new Path("/tmp/"), Array.empty)
      true
    } catch {
      case _: UnsupportedOperationException ⇒ false
      case _: Throwable                     ⇒ true
    }
  }

  private[storage] def getCsvFilePaths(fs: FileSystem, path: Path): Array[Path] = {
    def toList(it: RemoteIterator[LocatedFileStatus], arr: List[Path] = List.empty): List[Path] = {
      if (it.hasNext) {
        val nextPath = it.next().getPath
        toList(it, arr :+ nextPath)
      } else arr
    }

    toList(fs.listFiles(path, true))
      .filter(_.getName.endsWith(".csv"))
      .toArray
  }

  override def readFromParquet[T: Encoder](location: String, selectColumns: Seq[Column] = Seq()): Dataset[T] = {
    val parquetDF = spark
      .read
      .parquet(location)

    val parquetSelectDF = {
      if (selectColumns.nonEmpty) parquetDF.select(selectColumns: _*)
      else parquetDF
    }

    parquetSelectDF.as[T]
  }

  override def writeToParquet(ds: Dataset[_], location: String, partitionBy: Seq[String] = Seq()): Unit = {
    ds
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy(partitionBy: _*)
      .parquet(location)
  }
}
