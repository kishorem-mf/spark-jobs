package com.unilever.ohub.spark.recommandation

import util.control.Breaks._

case class SifuSelection(INFO_TYPE:String)

object SifuConverter extends App{

  val countryListEmakina = Array("AE","AE","AR","AT","AT","AT","AU","BE","BE","BG","BH","BH","BR","CA","CA","CH","CH","CL","CO","CR","CZ","DE","DK","EE","EG","EG","EG","ES","ET","FI","FI","FR","FR","GB","GR","GT","HN","HU","ID","ID","IE","IL","IT","JO","JO","KW","KW","LB","LB","LK","LT","LT","LT","LU","LV","LV","MV","MX","MY","MY","NI","NL","NO","NZ","OM","OM","PA","PH","PK","PK","PL","PT","PY","QA","QA","RO","RU","SA","SA","SE","SG","SG","SK","SV","TH","TH","TR","TR","US","VN","VN","ZA")
  val languageListEmakina = Array("ar","en","es","de","en","tr","en","fr","nl","bg","ar","en","pt","en","fr","de","fr","es","es","es","cs","de","da","et","ar","de","en","es","et","de","fi","fr","nl","en","el","es","es","hu","en","id","en","he","it","ar","en","ar","en","ar","en","en","et","lt","lv","fr","lt","lv","en","es","en","ms","es","nl","no","en","ar","en","es","en","en","ur","pl","pt","es","ar","en","ro","ru","ar","en","sv","en","zh","sk","es","en","th","nl","tr","en","en","vi","en")
  val countryLanguageListEmakina = countryListEmakina zip languageListEmakina.toList
  val PRODUCTS:SifuSelection = SifuSelection("products")
  val RECIPES:SifuSelection = SifuSelection("recipes")
  val MAX = 1000000

  Thread.sleep(1000)
  val productsJsonFormat = countryLanguageListEmakina
    .map(
      listValues => {
        var jsonString = ""
        var RANGE = 0
        breakable {
          for(i <- 1 to MAX by 100){
            if (getRestApiJsonString(createRestApiUrl(listValues._1, listValues._2, PRODUCTS, i, i + 100)) == "") break()
            RANGE = i + 100
          }
        }
        if(RANGE != 0) for(i <- 1 to RANGE by 100) {
          jsonString += getRestApiJsonString(createRestApiUrl(listValues._1, listValues._2, PRODUCTS, i, i + 100))
        }
        jsonString
      })
    .filter(listValue => listValue.nonEmpty)

  Thread.sleep(1000)
  val recipesJsonFormat = countryLanguageListEmakina
    .map(
      listValues => {
        var jsonString = ""
        var RANGE = 0
        breakable {
          for(i <- 1 to MAX by 100){
            if (getRestApiJsonString(createRestApiUrl(listValues._1, listValues._2, RECIPES, i, i + 100)) == "") break()
            RANGE = i + 100
          }
        }
        if(RANGE != 0) for(i <- 1 to RANGE by 100) {
          jsonString += getRestApiJsonString(createRestApiUrl(listValues._1, listValues._2, RECIPES, i, i + 100))
        }
        jsonString
      })
    .filter(listValue => listValue.nonEmpty)

  println("stop")

  def createRestApiUrl(countryCode:String,languageKey:String,sifuSelection:SifuSelection,startIndex:Int,endIndex:Int):String = {
    val baseUri = new java.net.URI("https://sifu.unileversolutions.com:443")
    baseUri.resolve(s"/${sifuSelection.INFO_TYPE}/$countryCode/$languageKey/$startIndex/$endIndex?type=${sifuSelection.INFO_TYPE.toUpperCase().substring(0,sifuSelection.INFO_TYPE.length - 1)}").toString
  }

  def getRestApiJsonString(url:String):String = {
    import scala.io.Source._
    try fromURL(url).mkString catch{
      case _: Throwable => ""
    }
  }
}
