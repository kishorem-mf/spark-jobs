package com.unilever.ohub.spark

import scala.io.Source
import org.apache.spark.sql._

trait DomainDataProvider {

  def sourcePreferences: Map[String, Int]
}

object DomainDataProvider {
  def apply(spark: SparkSession): DomainDataProvider =
    new InMemDomainDataProvider(spark)
}

class InMemDomainDataProvider(spark: SparkSession) extends DomainDataProvider with Serializable {

  override val sourcePreferences: Map[String, Int] = {
    Source
      .fromInputStream(this.getClass.getResourceAsStream("/data-sources.csv"))
      .getLines()
      .toSeq
      .filter(_.nonEmpty)
      .drop(1)
      .map(_.split(","))
      .map(lineParts ⇒ lineParts(0) -> lineParts(1).toInt)
      .toMap
  }
}
