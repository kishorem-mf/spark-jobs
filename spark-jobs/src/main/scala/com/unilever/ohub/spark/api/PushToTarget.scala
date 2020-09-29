package com.unilever.ohub.spark.api

import sys.process._
import com.unilever.ohub.spark.storage.Storage
//import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import util.control.Breaks._


case class PushToTarget(inputUrl: String = "input-file",
                        authorization: String = "authorization-for-url"
                       ) extends SparkJobConfig

object PushToTarget extends SparkJob[PushToTarget] {


  override private[spark] def defaultConfig = PushToTarget()

  override private[spark] def configParser(): OptionParser[PushToTarget] =
    new scopt.OptionParser[PushToTarget]("PushToTarget") {
      head("PushToTarget", "1.0")
      opt[String]("inputUrl") required() action { (x, c) ⇒
        c.copy(inputUrl = x)
      } text "inputFile is a string property"
      opt[String]("Authorization") required() action { (x, c) ⇒
        c.copy(authorization = x)
      } text "Authorization is a string property"

      version("1.0")
      help("help") text "help text"
    }


  def setRequestAndGetResponse(call_url:String,authorization:String,payload:Option[String]): String = {
    var url = call_url
    var raw_result:Option[String]=None

    val auth = authorization
    val http_headers = s"{'Authorization': 'Basic $auth','Content-Type': 'application/json; charset=utf-8'}"
    val http_status =
      s"""python3 -c "import requests;http_headers=$http_headers;
         |print(requests.get('$url', headers=http_headers, allow_redirects=False).status_code)
         | if($payload is None) else print(requests.post('$url', headers=http_headers, data=$payload, allow_redirects=False).status_code);" """.stripMargin

    val http_location =
      s"""python3 -c "import requests;http_headers=$http_headers;
         |print(requests.get('$url', headers=http_headers, allow_redirects=False).headers["Location"])
         | if($payload is None) else print(requests.post('$url', headers=http_headers, data=$payload, allow_redirects=False).headers["Location"]);" """.stripMargin

    val http_raw =
      s"""python3 -c "import requests;http_headers=$http_headers;
         |print(requests.get('$url', headers=http_headers, allow_redirects=False))
         | if($payload is None) else print(requests.post('$url', headers=http_headers, data=$payload, allow_redirects=False));" """.stripMargin

    breakable {

      while (true) {

        val status_raw=Seq("/bin/bash", "-c", http_status).!!
        val status_code=status_raw.filter(_ >= ' ')
        log.info(s"status_code is $status_code")

        if(status_code >= "301" && status_code <= "399"){
          val location_raw=Seq("/bin/bash", "-c", http_location).!!
          val redirectedUrl=location_raw.filter(_ >= ' ')
          url = redirectedUrl
          raw_result = None
        }else{
          val raw=Seq("/bin/bash", "-c", http_raw).!!
          raw_result=Some(raw.filter(_ >= ' '))

          break
        }
      }
      raw_result.getOrElse("No name given")
    }

    raw_result.getOrElse("No name given")
  }


  override def run(spark: SparkSession, config: PushToTarget, storage: Storage): Unit = {
    //import spark.implicits._

    val resp = setRequestAndGetResponse(config.inputUrl,config.authorization,None)

    //val productdataset = storage.readFromJson(resp.split("\n").toList.toDS())
    //storage.writeToParquet(productdataset, config.outputFile)
    log.info(s"Response Status is $resp")
  }
}
