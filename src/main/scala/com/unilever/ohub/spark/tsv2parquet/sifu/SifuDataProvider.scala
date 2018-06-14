package com.unilever.ohub.spark.tsv2parquet.sifu

import java.net.{ URI, URL }

import io.circe
import io.circe.Decoder
import io.circe.generic.auto._
import io.circe.parser._
import io.circe._
import io.circe.generic.semiauto._
import io.circe.generic.decoding._

import scala.io.Source
import scala.util.{ Failure, Success, Try }

trait SifuDataProvider {

  def productsFromApi(
    countryCode: String,
    languageKey: String,
    sifuSelection: String,
    pageSize: Int = 100,
    pages: Int = 10): Seq[SifuProductResponse]
}

class JsonSifuDataProvider() extends SifuDataProvider {

  def productsFromApi(
    countryCode: String,
    languageKey: String,
    sifuSelection: String,
    pageSize: Int = 100,
    pages: Int = 10
  ): Seq[SifuProductResponse] = {

    val totalNrOfResults = (pages - 1) * pageSize
    (0 to totalNrOfResults by pageSize).foldLeft(Seq.empty[SifuProductResponse]) {
      case (acc, i) ⇒
        val triedUrl = createSifuURL(countryCode, languageKey, sifuSelection, i, i + pageSize)
        val parsed: Seq[SifuProductResponse] = triedUrl match {
          case Failure(_) ⇒ Seq.empty[SifuProductResponse]
          case Success(url) ⇒
            val body = getResponseBodyString(url)
            implicit val decodeSifuProductResponse: Decoder[SifuProductResponse] = deriveDecoder[SifuProductResponse]
            //            implicit val decodeSeqSifuProductResponse: Decoder[Seq[SifuProductResponse]] = deriveDecoder[Seq[SifuProductResponse]]
            decodeJsonString[Seq[SifuProductResponse]](body).right.getOrElse(List.empty)
        }
        acc ++ parsed
    }
  }

  private[sifu] def getResponseBodyString(url: URL): String = Source.fromURL(url).mkString

  private[sifu] def decodeJsonString[Response: Decoder](body: String): Either[circe.Error, Response] = {
    decode[Response](body)
  }

  private[sifu] def createSifuURL(
    countryCode: String,
    languageKey: String,
    sifuSelection: String,
    startIndex: Int,
    endIndex: Int
  ): Try[URL] = Try {
    val baseUri = new URI("https://sifu.unileversolutions.com:443")
    val typeParam = sifuSelection.toUpperCase().substring(0, sifuSelection.length - 1)
    baseUri
      .resolve(s"/$sifuSelection/$countryCode/$languageKey/$startIndex/$endIndex?type=$typeParam")
      .toURL
  }
}
