package com.unilever.ohub.spark

import com.unilever.ohub.spark.domain.entity.ChannelReference

import scala.io.Source

trait DomainDataProvider {
  def sourcePreferences: Map[String, Int]

  def channelReferences: Map[String, ChannelReference]

  def sourceIds: Map[String, Int]
}

object DomainDataProvider {
  def apply(): DomainDataProvider =
    new InMemDomainDataProvider()
}

class InMemDomainDataProvider() extends DomainDataProvider with Serializable {

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

  override lazy val channelReferences: Map[String, ChannelReference] = {
    Source.fromInputStream(this.getClass.getResourceAsStream("/channel_references.csv"))
      .getLines()
      .toSeq
      .drop(1)
      .map(_.split(";"))
      .map(lineParts ⇒ ChannelReference(
        channelReferenceId = lineParts(0),
        socialCommercial = Some(lineParts(1)),
        strategicChannel = lineParts(2),
        globalChannel = lineParts(3),
        globalSubChannel = lineParts(4)
      ))
      .map(ref ⇒ ref.channelReferenceId -> ref)
      .toMap
  }

  override lazy val sourceIds: Map[String, Int] = {
    Source
      .fromInputStream(this.getClass.getResourceAsStream("/sources.csv"))
      .getLines()
      .toSeq
      .filter(_.nonEmpty)
      .drop(1)
      .map(_.split(";"))
      .map(lineParts ⇒ lineParts(0) -> lineParts(1).toInt)
      .toMap
  }
}
