package com.unilever.ohub.spark.recommandation

import util.control.Breaks._

case class SifuSelection(INFO_TYPE:String)

object SifuConverter extends App{
  if (args.length != 2) {
    println("specify PRODUCTS_FILE RECIPES_FILE")
    sys.exit(1)
  }

  val productsFile = args(0)
  val recipesFile = args(1)

  val startOfJob = System.currentTimeMillis()

  println(s"Generating SIFU_PRODUCTS parquet [$productsFile] and SIFU_RECIPES parquet [$recipesFile]")

  val countryListEmakina = Array("AE","AE","AF","AF","AR","AR","AT","AT","AT","AT","AU","AU","AU","AU","AU","AU","AU","AU","AU","AZ","AZ","BE","BE","BG","BH","BH","BR","BR","CA","CA","CH","CH","CL","CN","CN","CN","CN","CN","CN","CN","CN","CN","CN","CN","CN","CN","CN","CN","CN","CN","CN","CN","CN","CN","CN","CO","CR","CX","CZ","DE","DE","DE","DK","EE","EG","EG","EG","ES","ET","FI","FI","FR","FR","GB","GR","GT","HN","HU","ID","ID","ID","ID","ID","ID","ID","ID","ID","ID","IE","IE","IE","IE","IE","IE","IE","IE","IE","IE","IE","IE","IL","IN","IN","IT","JO","JO","KR","KR","KR","KR","KR","KR","KR","KR","KR","KW","KW","LB","LB","LK","LK","LK","LT","LT","LT","LU","LV","LV","MM","MV","MX","MX","MX","MX","MX","MX","MX","MX","MX","MX","MX","MX","MX","MX","MX","MX","MX","MX","MX","MX","MX","MY","MY","MY","NI","NI","NL","NL","NO","NZ","NZ","NZ","NZ","NZ","NZ","NZ","OM","OM","PA","PH","PH","PH","PH","PH","PH","PK","PK","PK","PL","PL","PT","PY","QA","QA","RO","RU","SA","SA","SE","SE","SG","SG","SG","SG","SG","SG","SK","SV","TH","TH","TH","TH","TR","TR","US","US","US","US","US","US","US","US","US","US","US","US","US","US","US","US","US","US","US","US","VN","VN","VN","ZA","ZA")
  val languageListEmakina = Array("ar","en","de","zh","es","fi","ab","de","en","tr","da","de","en","es","fi","he","pt","ru","tr","az","tr","fr","nl","bg","ar","en","fi","pt","en","fr","de","fr","es","ar","bg","cs","da","de","en","es","fi","fr","he","hu","it","lt","lv","nl","no","pl","pt","ru","sv","tr","zh","es","es","en","cs","de","en","fr","da","et","ar","de","en","es","et","de","fi","fr","nl","en","el","es","es","hu","de","en","es","fi","he","id","no","pt","tr","zh","da","de","en","es","fi","he","id","pt","sv","th","tr","zh","he","en","zh","it","ar","en","da","de","en","es","fi","ko","pl","pt","tr","ar","en","ar","en","en","si","zh","et","lt","lv","fr","lt","lv","my","en","bg","da","de","en","es","fi","fr","he","hu","id","it","lv","nl","no","pl","pt","ru","sv","th","tr","zh","en","ms","zh","en","es","fr","nl","no","da","de","en","es","fi","he","pt","ar","en","es","da","de","en","fi","tl","zh","en","ur","zh","en","pl","pt","es","ar","en","ro","ru","ar","en","sv","zh","en","es","fi","nl","tr","zh","sk","es","de","en","es","th","nl","tr","bg","da","de","en","es","fi","fr","he","hu","id","it","ko","nl","no","pl","pt","ru","sv","tr","zh","en","vi","zh","af","en")
  val countryLanguageListEmakina = countryListEmakina zip languageListEmakina.toList
  val PRODUCTS:SifuSelection = SifuSelection("products")
  val RECIPES:SifuSelection = SifuSelection("recipes")
  val MAX = 1000000
  val RANGE = 100

  val productsJsonFormat = countryLanguageListEmakina
    .map(listValues => getConcatenatedJsonString(listValues._1,listValues._2,PRODUCTS,RANGE,MAX))
    .filter(listValue => listValue.nonEmpty)

  Thread.sleep(10000) /* Pause for 10 seconds to make sure recipes are loaded from SIFU */
  val recipesJsonFormat = countryLanguageListEmakina
    .map(listValues => getConcatenatedJsonString(listValues._1,listValues._2,RECIPES,RANGE,MAX))
    .filter(listValue => listValue.nonEmpty)

  import org.apache.spark.sql.SaveMode._
  import org.apache.spark.sql._
  val spark = SparkSession.builder().appName("Get JSON and convert to Parquet").getOrCreate()
  import spark.implicits._

  try {
    if (productsJsonFormat(0).nonEmpty) println("Products are found.")
    if (recipesJsonFormat(0).nonEmpty) println("Recipes are found.")
  } catch {
    case _: Throwable => println("Not all data is found in SIFU. Job ends.") ; sys.exit(1)
  }

  val productsDF = createDataFrameFromJsonString(spark,productsJsonFormat)
  val recipesDF = createDataFrameFromJsonString(spark,recipesJsonFormat)

//  TODO create selection "queries" based on raw parquet needed for REC-O
  productsDF.write.mode(Overwrite).format("parquet").save(productsFile)
  recipesDF.write.mode(Overwrite).format("parquet").save(recipesFile)
  productsDF.printSchema()
  recipesDF.printSchema()

  println(s"Done in ${(System.currentTimeMillis - startOfJob) / 1000}s")

  def getConcatenatedJsonString(countryCode:String,languageKey:String,sifuSelection: SifuSelection,range: Int,maxIterations: Int): String = {
    var jsonString = ""
    var newRange = 0
    breakable {
      for (i <- 1 to maxIterations by range) {
        if (getRestApiJsonString(createRestApiUrl(countryCode, languageKey, sifuSelection, i, i + range)) == "") break()
        newRange = i + range
      }
    }
    if (newRange != 0) for (i <- 1 to newRange by range) {
      jsonString += getRestApiJsonString(createRestApiUrl(countryCode, languageKey, sifuSelection, i, i + range))
    }
    jsonString match {
      case value: String if value.nonEmpty => jsonString
      case _ => ""
    }
  }

  def getRestApiJsonString(url:String):String = {
    import scala.io.Source._
    try fromURL(url).mkString catch{
      case _: Throwable => ""
    }
  }

  def createRestApiUrl(countryCode:String,languageKey:String,sifuSelection:SifuSelection,startIndex:Int,endIndex:Int):String = {
    val baseUri = new java.net.URI("https://sifu.unileversolutions.com:443")
    baseUri.resolve(s"/${sifuSelection.INFO_TYPE}/$countryCode/$languageKey/$startIndex/$endIndex?type=${sifuSelection.INFO_TYPE.toUpperCase().substring(0,sifuSelection.INFO_TYPE.length - 1)}").toString
  }

  def createDataFrameFromJsonString(spark:SparkSession, jsonStrings:Array[String]):DataFrame = {
    val jsonString = jsonStrings.toSeq
    spark.sparkContext.parallelize(jsonString).toDS()
  }
}
