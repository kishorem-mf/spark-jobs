package com.unilever.ohub.spark.tsv2parquet

import java.io.{ File, InputStream }
import java.nio.file.{ Path, Paths }

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.tsv2parquet.emakina._
import com.unilever.ohub.spark.tsv2parquet.file_interface._
import com.unilever.ohub.spark.tsv2parquet.fuzzit._
import org.apache.commons.lang.WordUtils
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.sql.SparkSession
import org.scalatest.{ FunSpec, Matchers }

class ConverterDataIngestionTest extends FunSpec with Matchers { self ⇒
  implicit protected val log: Logger = LogManager.getLogger(self.getClass)
  val spark: SparkSession = SparkSession.builder().appName("Test converters").getOrCreate()
  val testFolderPath = "src/test/resources"
  val tempFolderPath: Path = Paths.get(System.getProperty("java.io.tmpdir"))
  val fileNamesShort: Array[String] =
    Array[String](
      "FILE_CONTACTPERSON_ACTIVITIES", "FILE_CONTACTPERSON_SOCIAL", "FILE_CONTACTPERSONCLASSIFICATIONS", "FILE_CONTACTPERSONS", "FILE_OPERATOR_ACTIVITIES", "FILE_OPERATORCLASSIFICATIONS", "FILE_OPERATORS", "FILE_ORDERS", "FILE_PRODUCTS",
      "EMAKINA_ACTIVITIES", "EMAKINA_ANSWERS", "EMAKINA_COMPETITION_ENTRIES", "EMAKINA_CONTACT_PERSONS", "EMAKINA_CONTACT_REQUESTS", "EMAKINA_LOYALTY_BALANCE", "EMAKINA_ORDERS", "EMAKINA_ORDER_ITEMS", "EMAKINA_QUESTIONS", "EMAKINA_RES_DOWNLOAD_REQUESTS", "EMAKINA_SAMPLE_ORDERS", "EMAKINA_SUBSCRIPTION_REQUESTS", "EMAKINA_WEB_ORDERS", "EMAKINA_WEB_ORDER_ITEMS", "EMAKINA_WEBSERVICE_REQUESTS",
      "FUZZIT_OPERATORS", "FUZZIT_PRODUCTS", "FUZZIT_ORDERLINES"
    )

  val fileNamesFull: Array[String] = fileNamesShort.map(fileName ⇒ s"$testFolderPath/$fileName.csv")
  val converters: Array[SparkJob] =
    Array[SparkJob](
      FileContactPersonActivityConverter, FileContactPersonSocialConverter, FileContactPersonClassConverter, FileContactPersonConverter, FileOperatorActivityConverter, FileOperatorClassConverter, FileOperatorConverter, FileOrderConverter, FileProductConverter,
      EmakinaActivityConverter, EmakinaAnswerConverter, EmakinaCompetitionEntryConverter, EmakinaContactPersonConverter, EmakinaContactRequestConverter, EmakinaLoyaltyBalanceConverter, EmakinaOrderConverter, EmakinaOrderItemConverter, EmakinaQuestionConverter, EmakinaResDownloadRequestConverter, EmakinaSampleOrderConverter, EmakinaSubscriptionRequestConverter, EmakinaWebOrderConverter, EmakinaWebOrderItemConverter, EmakinaWebserviceRequestConverter,
      FuzzitOperatorConverter, FuzzitProductConverter, FuzzitSaleConverter
    )

  converters.zip(fileNamesShort.zip(fileNamesFull)).foreach {
    convertersWithFileNames ⇒
      val tempFileName = tempFolderPath.resolve("out.parquet").toString
      val filePaths: Product = (convertersWithFileNames._2._2, tempFileName)
      val shortFileName = WordUtils.capitalize(convertersWithFileNames._2._1.toLowerCase(), "_".toCharArray).replace("_", "")
      val converter = convertersWithFileNames._1
      describe(s"convertCorrect${shortFileName}FileToParquet") {
        it(s"should convert the correctly formatted $shortFileName file into a parquet file") {
          assert(
            runTestConverter(spark, converter, filePaths, tempFileName)
          )
        }
      }
  }

  private def runTestConverter(spark: SparkSession, converter: SparkJob, filePaths: List[String], tempFileName: String): Boolean = {
    try {
      if (new File(tempFileName).exists()) new File(tempFileName).delete()
      Thread.sleep(2000)
      converter.run(spark, filePaths, new com.unilever.ohub.spark.storage.DefaultStorage(spark))
      log.info(s"${filePaths.productElement(0)} successfully tested")
      true
    } catch {
      case _: Throwable ⇒ false
    }
  }
}
