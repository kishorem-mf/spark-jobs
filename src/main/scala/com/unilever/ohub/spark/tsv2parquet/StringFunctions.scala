package com.unilever.ohub.spark.tsv2parquet

import scala.sys.process._

object StringFunctions extends App{
  val startOfJob = System.currentTimeMillis()
  val COUNT = 1 //Levenshtein 1 billion in 10 minutes
  print(cleanString("null"))
//  val charArrayFirst = "wddwdw".toCharArray //"Συγχαρητήρια"
//  val charArraySecond = "wdwddw".toCharArray //"Συγχαρητχαήρια"
//  for (_ <- 0 until COUNT) getFastSimilarity(charArrayFirst,charArraySecond)
//  println(getFastSimilarity(charArrayFirst,charArraySecond))
//  println(charArrayFirst + ":" + charArraySecond)
  println(s"Processed $COUNT records in ${(System.currentTimeMillis - startOfJob) / 1000}s")

  def calculateSimilarityCPlusPlus(sourceString:Array[Char],targetString:Array[Char]):Double = {
    val command = "C:\\Users\\roderik-von.maltzahn\\Documents\\1_roderik\\1_crm\\07_projects\\15_ohub_improvement\\03_ohub_2.0\\09_code\\string_similarity.exe " + sourceString.toString + " " + targetString.toString
    val output = command.!!
    output.toDouble
  }

  def calculateSimilarity(first:String, second:String, setToLower:Boolean = false, lengthThreshold:Int = 6):Double = {
    if(first == null || second == null) return 0.0
    if(first == second) return 1.0

    val staysFirstString = if(first.length == second.length) first < second else if (first.length < second.length) true else false
    var firstString:String = null
    var secondString:String = null
    if(staysFirstString) {
      firstString = if(setToLower) first.toLowerCase() else first
      secondString = if(setToLower) second.toLowerCase() else second
    } else {
      firstString = if(setToLower) second.toLowerCase() else second
      secondString = if(setToLower) first.toLowerCase() else first
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

      calculateSimilarityToPower(math.max(matchPairCounter/pairDenominator, totalSinglesSameIndex / (lengthFirstString + lengthDifference)))
    } catch {
      case _: Exception => throw new Exception("first: ".concat(new String(firstString)).concat("; second: ").concat(new String(secondString)))
    }
  }

  def calculateLevenshtein(first:CharSequence, second:CharSequence, setToLower:Boolean = false,addDamerau:Boolean = false):Double = {
    if(first == null || second == null) return 0.0
    if(first == second) return 1.0

    val lengthFirst = first.length()
    val lengthSecond = second.length()
    if(lengthFirst == 0) return lengthSecond
    if(lengthSecond == 0) return lengthFirst
    val distanceMatrix = Array.ofDim[Int](lengthFirst,lengthSecond)
    for(i <- 0 until lengthFirst) distanceMatrix(i)(0) = i
    for(i <- 0 until lengthSecond) distanceMatrix(0)(i) = i
    var isTransposition = false

    for(row <- 1 until lengthFirst) {
      val charRowFirst = first.charAt(row)
      for(column <- 1 until lengthSecond) {
        val charColumnSecond = second.charAt(column)
        if(addDamerau) {
          isTransposition = false //if(setToLower) row > 1 && column > 1 && charRowFirst.toLower == second.charAt(column - 1).toLower && first.charAt(row - 1).toLower == charColumnSecond.toLower
                                  //else row > 1 && column > 1 && charRowFirst == second.charAt(column - 1) && first.charAt(row - 1) == charColumnSecond
        }
        val costNoMatch = if(setToLower) {
          if(charRowFirst.toLower == charColumnSecond.toLower) 0 else 1
        } else {
          if(charRowFirst == charColumnSecond) 0 else 1
        }

        val valuePreviousFirst = distanceMatrix(row - 1)(column) + 1
        val valuePreviousSecond = distanceMatrix(row)(column - 1) + 1
        val valuePreviousBoth = distanceMatrix(row - 1)(column - 1) + costNoMatch

        if(isTransposition) {
          distanceMatrix(row)(column) = math.min(distanceMatrix(row)(column),distanceMatrix(row - 2)(column - 2) + costNoMatch)
          isTransposition = false
        } else {
          distanceMatrix(row)(column) = math.min(valuePreviousFirst,math.min(valuePreviousSecond,valuePreviousBoth))
        }
      }
    }
    val maxLength:Double = math.max(lengthFirst,lengthSecond).toDouble
    (maxLength - distanceMatrix(lengthFirst - 1)(lengthSecond - 1).toDouble) / maxLength
  }

  def getFastSimilarity(first:Array[Char], second:Array[Char]):Double = {
    if (first == null || second == null) return 0.0
    if (first.sameElements(second)) return 1.0

    val staysFirstString = if(first.length == second.length) true else if (first.length < second.length) true else false
    var firstString:Array[Char] = null
    var secondString:Array[Char] = null
    if(staysFirstString) {
      firstString = first
      secondString = second
    } else {
      firstString = second
      secondString = first
    }

    val lengthFirstString = firstString.length
    val lengthSecondString = secondString.length
    val lengthDifference = lengthSecondString - lengthFirstString

    if(lengthFirstString == 1) {
      lengthSecondString match {
        case 1.0 => return 0.0
        case _ => return 1.0 / lengthSecondString.toDouble
      }
    }
    var matchPairCounterFirstString:Double = 0.0
    var matchPairCounterSecondString:Double = 0.0
    val firstArray = new Array[Int](lengthFirstString - 1)
    val secondArray = new Array[Int](lengthSecondString - 1)
    for(i <- 1 until lengthFirstString) firstArray(i - 1) = (firstString(i - 1).toInt * firstString(i - 1).toInt) + (firstString(i).toInt * firstString(i).toInt)
    for(i <- 1 until lengthSecondString) secondArray(i - 1) = (secondString(i - 1).toInt * secondString(i - 1).toInt) + (secondString(i).toInt * secondString(i).toInt)
    for (i <- 0 until lengthFirstString.toInt - 1) {
      var duplicateCounterFirst = 0
      var duplicateCounterSecond = 0
      for (j <- 0 until lengthSecondString.toInt - 1) {
        if(j < lengthFirstString - 1 && firstArray(i) == firstArray(j)) {
          duplicateCounterFirst += 1
        }
        if(firstArray(i) == secondArray(j)) {
          matchPairCounterSecondString += 1.0
          duplicateCounterSecond += 1
        }
      }
//      Handle duplicates in first and second string
      if(duplicateCounterSecond > 1) {
        matchPairCounterSecondString -= math.abs(duplicateCounterFirst - duplicateCounterSecond)
      }
      if(duplicateCounterFirst > 1) {
        if(duplicateCounterSecond > 1) {
          matchPairCounterSecondString -= (duplicateCounterFirst - 1)
        } else {
          matchPairCounterSecondString -= (1.0 - 1.0 / duplicateCounterFirst.toDouble)
        }
      }

    }
    matchPairCounterFirstString = lengthFirstString - 1

    val differenceCount = lengthDifference + (matchPairCounterFirstString - matchPairCounterSecondString)
    if(differenceCount > lengthFirstString) {
      0                                                                 /* Correct for small first and larger second string */
    } else if(differenceCount == 0) {
      (lengthFirstString - 1).toDouble / lengthFirstString.toDouble     /* Correct inverse matches resulting in equality */
    } else {
      1 - differenceCount.toDouble / lengthFirstString.toDouble         /* Taking length into account as well for final number */
    }
  }

  def cleanString(input:String):String = {
    try{
      if(input == null) null else input.toLowerCase().replaceAll("(^\\s*)|(\\s*$)|[$₠₡₢₣₤₥₦₧₨₩₪₫€₭₮₯₰₱₲₳₴₵₶₷₸\u0081°”\\\\_\\'\\~`!@#$%()={}|:;\\?/<>,\\.\\[\\]\\+\\-\\*\\^&:0-9]+", "")
    } catch {
      case _: Exception => throw new Exception("string: ".concat(new String(input)))
    }
  }
}
