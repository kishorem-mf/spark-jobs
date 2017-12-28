package com.unilever.ohub.spark.tsv2parquet

import scala.sys.process._

object StringFunctions extends App {
  val startOfJob = System.currentTimeMillis()
  val COUNT = 1000000
  val charArrayFirst = "Συγχαρητήρια"
  val charArraySecond = "Συγχαρητχαήρια"
  for (_ <- 0 until COUNT) calculateSimilarity(charArrayFirst,charArraySecond)
  println(calculateSimilarity(charArrayFirst,charArraySecond))
  println(charArrayFirst + ":" + charArraySecond)
  println(s"Processed $COUNT records in ${(System.currentTimeMillis - startOfJob) / 1000}s")

  def calculateSimilarityCPlusPlus(sourceString:Array[Char],targetString:Array[Char]):Double = {
    val command = "C:\\Users\\roderik-von.maltzahn\\Documents\\1_roderik\\1_crm\\07_projects\\15_ohub_improvement\\03_ohub_2.0\\09_code\\string_similarity.exe " + sourceString.toString + " " + targetString.toString
    val output = command.!!
    output.toDouble
  }

  def calculateSimilarity(first:String, second:String, setToLower:Boolean = true, lengthThreshold:Int = 6):Double = {
    if(first == null || second == null) return 0.0
    val firstInput = if(setToLower) first.toLowerCase() else first
    val secondInput = if(setToLower) second.toLowerCase() else second
    if(firstInput == secondInput) return 1.0
    val staysFirstString = if(firstInput.length == secondInput.length) firstInput < secondInput else if (firstInput.length < secondInput.length) true else false
    var firstString:String= firstInput
    var secondString:String = secondInput
    if(staysFirstString) {
      firstString = firstInput
      secondString = secondInput
    } else {
      firstString = secondInput
      secondString = firstInput
    }

    val lengthFirstString = firstString.length.toDouble
    val lengthSecondString = secondString.length.toDouble
    val lengthDifference = math.abs(lengthFirstString - lengthSecondString)
    if(lengthSecondString < lengthThreshold) return -1.0

    if(lengthFirstString == 1) {
      lengthSecondString match {
        case 1.0 => return 0.0
        case _ => return 1.0 / lengthSecondString
      }
    }

    try {
      var matchPairCounter: Double = 0.0
      var totalSinglesSameIndex: Double = 0.0
      for (i <- 0 until lengthFirstString.toInt) {
        val firstCharOne =  firstString.charAt(i)
        val secondCharOne = if(i < lengthFirstString - 1) firstString.charAt(i + 1) else '‰'
        val sameIndexFirstCharTwo = secondString.charAt(i)
        if (firstCharOne == sameIndexFirstCharTwo) totalSinglesSameIndex += 1.0

        var isCountedPair = false
        for (j <- 0 until lengthSecondString.toInt) {
          val firstCharTwo = secondString.charAt(j)
          val secondCharTwo = if(j < lengthSecondString.toInt - 1) secondString.charAt(j + 1) else null
          val isDoubleMatch = firstCharOne == firstCharTwo && secondCharOne == secondCharTwo
          if (isDoubleMatch && !isCountedPair) {
            matchPairCounter += 1.0
            isCountedPair = true
          }
        }
      }

//      Calculate how many times Similarity must by multiplied with itself to correct for big string comparisons
      val CORRECTION:Int = 30
      val POWER:Int = 1 + ((lengthFirstString.toInt - (lengthFirstString.toInt / CORRECTION)) / CORRECTION)
      val pairDenominator = if(lengthFirstString == lengthSecondString) lengthDifference + lengthFirstString else lengthDifference + lengthFirstString - 1
      def calculateSimilarityToPower(similarity:Double,input:Int = POWER):Double = {
        Math.pow(similarity,input)
      }

      calculateSimilarityToPower(Math.max(matchPairCounter/pairDenominator, totalSinglesSameIndex / (lengthFirstString + lengthDifference)))
    } catch {
      case _: Exception => throw new Exception("first: ".concat(new String(firstString)).concat("; second: ").concat(new String(secondString)))
    }
  }
}
