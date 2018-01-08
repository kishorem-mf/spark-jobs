package com.unilever.ohub.spark.matching

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.unilever.ohub.spark.tsv2parquet.StringFunctions

object LevenshteinOperatorMatch extends App {

  if (args.length != 2) {
    println("specify INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val inputFile = args(0)
  val outputFile = args(1)

  val spark = SparkSession.builder().getOrCreate()
  spark.sqlContext.udf.register("REGEXPR_REPLACE",(s1:String) => s1 match {case null => null;case _ => s1.toLowerCase().replaceAll("(^\\s*)|(\\s*$)|[$₠₡₢₣₤₥₦₧₨₩₪₫€₭₮₯₰₱₲₳₴₵₶₷₸\u0081°”\\\\_\\'\\~`!@#$%()={}|:;\\?/<>,\\.\\[\\]\\+\\-\\*\\^&:0-9]+", "")})
  spark.sqlContext.udf.register("SIMILARITY",(s1:String,s2:String) => StringFunctions.getFastSimilarity(s1 match {case null => null;case _ => s1.toCharArray},s2 match {case null => null;case _ => s2.toCharArray}))
  val operatorsDF1 = spark.read.parquet(inputFile)
  val operatorsDF2 = operatorsDF1 //.where("country_code = 'IE'")
  operatorsDF2.createOrReplaceTempView("operators2")
  val operatorsDF3 = spark.sql(
    """
      |select ref_operator_id id,name,lower(regexpr_replace(name)) clean_name
      |from operators2
    """.stripMargin)
  operatorsDF3.createOrReplaceTempView("operators3")
  val operatorsDF4 = spark.sql(
    """
      |select id,name,clean_name,substring(clean_name,1,3) block
      |from operators3
    """.stripMargin)
  operatorsDF4.createOrReplaceTempView("operators4")
  val operatorsDF5 = spark.sql(
    """
      |select id,name,clean_name,block
      |from operators4
      |order by block
    """.stripMargin)
  operatorsDF5.createOrReplaceTempView("operators5")
  val operatorsDF6 = spark.sql(
    """
      |select source.id source_id,target.id target_id,source.name source_name,target.name target_name,source.clean_name source_clean,target.clean_name target_clean,similarity(source.clean_name,target.clean_name) similarity,source.block block
      |from operators5 source
      |inner join operators5 target
      | on source.block = target.block
      |where 1 = 1
      | and similarity(source.clean_name,target.clean_name) > 0.8
      | and source.clean_name < target.clean_name
      |order by source_clean
    """.stripMargin)

  operatorsDF6.write.mode(SaveMode.Overwrite).format("parquet").save(outputFile)
//  operatorsDF6.show()

  println("Done")

}
