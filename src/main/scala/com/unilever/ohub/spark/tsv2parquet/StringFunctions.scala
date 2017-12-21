package com.unilever.ohub.spark.tsv2parquet

import java.lang.IndexOutOfBoundsException

object StringFunctions extends App {
  for (i <- 0 to 1000000) calculateSimilarity("test".toCharArray,"roderick".toCharArray)
  println(calculateSimilarity(null,"roderick".toCharArray))
  def calculateSimilarity(sourceString:Array[Char],targetString:Array[Char]):Double = {
    def parseCharArrayOption(input:Array[Char]):Option[Array[Char]] = {
      input match {
        case null => None
        case _ => Some(input)
      }
    }

    var lengthSource = 0
    parseCharArrayOption(sourceString) match {
      case None => lengthSource = 0
      case _ => lengthSource = sourceString.length
    }

    var lengthTarget = 0
    parseCharArrayOption(targetString) match {
      case None => lengthTarget = 0
      case _ => lengthTarget = targetString.length
    }

//    val lengthSource = parseCharArrayOption(sourceString).get.length
//    val lenghtTarget = parseCharArrayOption(targetString).length
    val CORRECTION = 30

//    Decide which should be the source string and set variables
    val lengthSourceMinusTarget = lengthSource - lengthTarget
    var sourceIsLonger = lengthSourceMinusTarget > 0
    val lengthIsEqual = lengthSourceMinusTarget == 0
    var firstString:Array[Char] = null
    var secondString:Array[Char] = null
    var lengthFirstString:Int = 0
    var lengthSecondString:Int = 0

    if (lengthSourceMinusTarget == 0) {
      for(
          i <- 0 until lengthSource
          if sourceString(i) > targetString(i)
      ) yield sourceIsLonger = true
//      Return 1 when equal
      if (!sourceIsLonger && sourceString.sameElements(targetString)) 1
    }

//    Set the first and the second string
    if (sourceIsLonger) {
      firstString = targetString
      secondString = sourceString
      lengthFirstString = lengthTarget
      lengthSecondString = lengthSource
    } else {
      firstString = sourceString
      secondString = targetString
      lengthFirstString = lengthSource
      lengthSecondString = lengthTarget
    }

//    Calculate how many times Similarity must by multiplied with itself to correct for big string comparisons
    val POWER:Int = 1 + ((lengthFirstString - (lengthFirstString / CORRECTION)) / CORRECTION)
    var similarity:Double = 0.0
    def calculateSimilarityToPower(similarity:Double,input:Int = POWER):Double = {
      Math.pow(similarity,input)
    }
    try {
//    Deal with the case when source is 1 char and target is many chars
      if (lengthFirstString == 1) {
        for(
            i <- 0 until lengthTarget
            if firstString(0).toLower.equals(secondString(0).toLower)
        ) yield return calculateSimilarityToPower(lengthFirstString.toDouble/lengthSecondString.toDouble)
      }

//    Calculate total counts
      var sameCharAtSameIndexBothStringsTotal:Int = 0
      var samePairInSourceStringCount:Int = 0
      var samePairInTargetStringCount:Int = 0
      var samePairInSourceStringTotal:Int = 0
      var samePairInTargetStringTotal:Int = 0
      for (i <- 0 until lengthFirstString - 1) {
//      1. Count chars at same index in both strings
        if (firstString(i).toLower.equals(secondString(i).toLower)) sameCharAtSameIndexBothStringsTotal += 1
//      2. Check occurrence of pairs
        for (j <- 0 until (lengthFirstString - 1)) {
//        First string
          if (
            firstString(i).toLower.equals(firstString(j).toLower)
            &&
            firstString(i + 1).toLower.equals(firstString(j + 1).toLower)
          ) samePairInSourceStringCount += 1
//        Second string
          if (
            firstString(i).toLower.equals(secondString(j).toLower)
            &&
            firstString(i + 1).toLower.equals(secondString(j + 1).toLower)
          ) samePairInTargetStringCount += 1
        }
//      3. Calculate total of matched pairs
        if (samePairInTargetStringCount > samePairInSourceStringCount) samePairInTargetStringCount = samePairInSourceStringCount
        samePairInSourceStringTotal = samePairInSourceStringCount
        samePairInTargetStringTotal = samePairInTargetStringCount
      }

//    Calculate similarity if not calculated
      val relativeSizeFirstString:Double = lengthFirstString.toDouble / lengthSecondString.toDouble
      var pairMatchRatio:Double = {
          if (
            ((samePairInSourceStringTotal - samePairInTargetStringTotal) > 1)
            &&
            (relativeSizeFirstString != 1.0)
          ) {
            samePairInTargetStringTotal.toDouble / samePairInSourceStringTotal.toDouble
          } else {
            samePairInTargetStringTotal.toDouble / samePairInSourceStringTotal.toDouble
          }
        }
      val sameIndexMatchRatio:Double = sameCharAtSameIndexBothStringsTotal.toDouble / lengthSecondString.toDouble
      val hasMoreIndexMatches = pairMatchRatio < sameIndexMatchRatio
      val hasOneOrMoreMatches = (samePairInTargetStringTotal + sameCharAtSameIndexBothStringsTotal) != 0

      if (hasMoreIndexMatches) {
        similarity = sameIndexMatchRatio * relativeSizeFirstString
      } else {
        if (hasOneOrMoreMatches) {
          similarity = pairMatchRatio * relativeSizeFirstString
        } else {
          similarity = 0
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new Exception("first: ".concat(new String(firstString)).concat("; second: ").concat(new String(secondString)))

    }
    calculateSimilarityToPower(similarity)

  }
}
