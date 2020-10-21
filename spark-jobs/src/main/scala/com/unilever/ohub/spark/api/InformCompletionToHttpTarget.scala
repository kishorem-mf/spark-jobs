package com.unilever.ohub.spark.api

import sys.process._
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import util.control.Breaks._


case class InformCompletionToHttpTarget(inputUrl: String = "input-file",
                                        authorization: String = "authorization-for-url",
                                        targetType: String = "Source-Target-Name",
                                        outputFolder: String ="path-to-output-file"
                                       ) extends SparkJobConfig

object InformCompletionToHttpTarget extends SparkJob[InformCompletionToHttpTarget] {


  override private[spark] def defaultConfig = InformCompletionToHttpTarget()

  override private[spark] def configParser(): OptionParser[InformCompletionToHttpTarget] =
    new scopt.OptionParser[InformCompletionToHttpTarget]("InformCompletionToHttpTarget") {
      head("InformCompletionToHttpTarget", "1.0")
      opt[String]("inputUrl") required() action { (x, c) ⇒
        c.copy(inputUrl = x)
      } text "inputFile is a string property"
      opt[String]("authorization") required() action { (x, c) ⇒
        c.copy(authorization = x)
      } text "authorization is a string property"
      opt[String]("targetType") required() action { (x, c) ⇒
        c.copy(targetType = x)
      } text "targetType is a string property"
      opt[String]("outputFolder") required() action { (x, c) ⇒
        c.copy(outputFolder = x)
      } text "outputFolder is a string property"


      version("1.0")
      help("help") text "help text"
    }


  def setRequestAndGetResponse(call_url:String,authorization:String,payload:Option[String]): String = {
    var url = call_url
    var raw_result:Option[String]=None

    val auth = authorization
    val http_headers = s"{'Authorization': 'Basic $auth','Content-Type': 'application/json; charset=utf-8'}"

    breakable {
      while (true) {

        val http_status =
          s"""python3 -c "import requests;http_headers=$http_headers;
             |print(requests.get('$url', headers=http_headers, allow_redirects=False).status_code) \\
             | if($payload is None) else print(requests.post('$url', headers=http_headers, data=$payload, allow_redirects=False).status_code);" """.stripMargin

        val status_raw=Seq("/bin/bash", "-c", http_status).!!
        val status_code=status_raw.filter(_ >= ' ')

        if(status_code >= "301" && status_code <= "399"){

          val http_location =
            s"""python3 -c "import requests;http_headers=$http_headers;
               |print(requests.get('$url', headers=http_headers, allow_redirects=False).headers["Location"]) \\
               | if($payload is None) else print(requests.post('$url', headers=http_headers, data=$payload, allow_redirects=False).headers["Location"]);" """.stripMargin

          val location_raw=Seq("/bin/bash", "-c", http_location).!!
          val redirectedUrl=location_raw.filter(_ >= ' ')
          url = redirectedUrl
          raw_result = None

        }else{

          val http_raw =
            s"""python3 -c "import requests;http_headers=$http_headers;
               |print(requests.get('$url', headers=http_headers, allow_redirects=False)) \\
               | if($payload is None) else print(requests.post('$url', headers=http_headers, data=$payload, allow_redirects=False));" """.stripMargin

          val raw=Seq("/bin/bash", "-c", http_raw).!!
          raw_result=Some(raw.filter(_ >= ' '))

          break
        }
      }
      raw_result.getOrElse("No name given")
    }

    raw_result.getOrElse("No name given")
  }


  override def run(spark: SparkSession, config: InformCompletionToHttpTarget, storage: Storage): Unit = {
    import spark.implicits._

    val specificUrl = config.inputUrl.split(";")
    val specificAuth = config.authorization.split(";")
    val specificType = config.targetType.split(";")

    var resp = "202"

    if((specificUrl.length == specificAuth.length) && (specificAuth.length == specificType.length) ){

      for(i <- 0 until specificUrl.length) {

        resp = setRequestAndGetResponse(specificUrl(i), specificAuth(i), None)
        storage.writeToParquet(storage.readFromJson(resp.split("\n").toList.toDS()), config.outputFolder + specificType(i) + ".parquet")

      }

    }else{
      throw new Exception("target_url, url_authorization, target_type length is not matching")
    }

  }
}
