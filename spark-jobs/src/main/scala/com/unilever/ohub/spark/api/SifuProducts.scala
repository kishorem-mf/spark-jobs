package com.unilever.ohub.spark.api

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser
import scala.util.{Try, Success, Failure}
import com.unilever.ohub.spark.api.SifuApiUtils._
import com.unilever.ohub.spark.api.SifuApiUtils.COUNTRIES.COUNTRIES

case class SifuProducts(inputUrl: String = "input-file",
                        outputFile: String = "path-to-output-file"
                       ) extends SparkJobConfig

object SifuProducts extends SparkJob[SifuProducts] {


  override private[spark] def defaultConfig = SifuProducts()

  override private[spark] def configParser(): OptionParser[SifuProducts] =
    new scopt.OptionParser[SifuProducts]("Sifu Products") {
      head("SifuProducts", "1.0")
      opt[String]("inputUrl") required() action { (x, c) ⇒
        c.copy(inputUrl = x)
      } text "inputFile is a string property"
      opt[String]("outputFile") required() action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }


  def getSifuApiContent(url: String): Try[String] = Try {
    scala.io.Source.fromURL(url).mkString
  }


  def addSkipAndTakeToBaseUrls(baseUrls: List[String], iteration: Int): List[String] = {
    val take = 100
    val skipList = 0 to (iteration * take) by take
    val a = baseUrls.map {
      value => s"S{value}"
    }
    val listOfValidUrls = skipList.flatMap {
      i =>
        baseUrls.map {
          value => s"${value}/${i}/${take}"
        }
    }
    listOfValidUrls.toList
  }

  def getValidSifuApiBaseUrls(countryCode: COUNTRIES,
                              inputUrl: String
                             ): List[String] = {
    val isDachCountry = List("DE", "CH", "AT").contains(countryCode.toString)
    val baseUrl =
      if (isDachCountry) {
        //      {"http://sifu.unileversolutions.com/products/pnir/"}
        s"${inputUrl}/pnir/"
      }
      //      {"http://sifu.unileversolutions.com/products/"}
      else {
        s"${inputUrl}/"
      }

    val countryCodes = countryLanguageMap(countryCode)
    val allBaseUrls = countryCodes.map { country =>
      s"${baseUrl}${countryCode}/${country}/0/1"
      }
    val allBaseUrlsRawOutput =
      allBaseUrls.map(url =>
        url -> getSifuApiContent(url))

    val allValidBaseUrls = allBaseUrlsRawOutput
      .map { value =>
        value._2 match {
          case Success(body) => value._1.toString.split("/").dropRight(2).mkString("", "/", "")
          case Failure(error) => "error"
        }
      }
      .filter { value =>
        value != "error"
      }
    allValidBaseUrls
  }


  override def run(spark: SparkSession, config: SifuProducts, storage: Storage): Unit = {
    import spark.implicits._
    val urls = sifuCountries.flatMap {
      country =>
        addSkipAndTakeToBaseUrls(getValidSifuApiBaseUrls(country, config.inputUrl), iteration)
    }
    var jsonSet = Set[String]()
    urls.foreach(url => {
      val productContent = getSifuApiContent(url).getOrElse("")
      jsonSet += productContent
    })

    val productdataset = storage.readFromJson(jsonSet.toSeq.toDS())
    storage.writeToParquet(productdataset, config.outputFile)
  }
}
