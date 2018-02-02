package com.unilever.ohub.spark.generic

object StringFunctions extends App {
  def calculateLevenshtein(first: CharSequence, second: CharSequence, setToLower: Boolean = false, addDamerau: Boolean = false): Double = {
    if (first == null || second == null) return 0.0
    if (first == second) return 1.0

    val lengthFirst = first.length()
    val lengthSecond = second.length()
    if (lengthFirst == 0) return lengthSecond
    if (lengthSecond == 0) return lengthFirst
    val distanceMatrix = Array.ofDim[Int](lengthFirst, lengthSecond)
    for (i <- 0 until lengthFirst) distanceMatrix(i)(0) = i
    for (i <- 0 until lengthSecond) distanceMatrix(0)(i) = i
    var isTransposition = false

    for (row <- 1 until lengthFirst) {
      val charRowFirst = first.charAt(row)
      for (column <- 1 until lengthSecond) {
        val charColumnSecond = second.charAt(column)
        if (addDamerau) {
          isTransposition = false //if(setToLower) row > 1 && column > 1 && charRowFirst.toLower == second.charAt(column - 1).toLower && first.charAt(row - 1).toLower == charColumnSecond.toLower
          //else row > 1 && column > 1 && charRowFirst == second.charAt(column - 1) && first.charAt(row - 1) == charColumnSecond
        }
        val costNoMatch = if (setToLower) {
          if (charRowFirst.toLower == charColumnSecond.toLower) 0 else 1
        } else {
          if (charRowFirst == charColumnSecond) 0 else 1
        }

        val valuePreviousFirst = distanceMatrix(row - 1)(column) + 1
        val valuePreviousSecond = distanceMatrix(row)(column - 1) + 1
        val valuePreviousBoth = distanceMatrix(row - 1)(column - 1) + costNoMatch

        if (isTransposition) {
          distanceMatrix(row)(column) = math.min(distanceMatrix(row)(column), distanceMatrix(row - 2)(column - 2) + costNoMatch)
          isTransposition = false
        } else {
          distanceMatrix(row)(column) = math.min(valuePreviousFirst, math.min(valuePreviousSecond, valuePreviousBoth))
        }
      }
    }
    val maxLength: Double = math.max(lengthFirst, lengthSecond).toDouble
    (maxLength - distanceMatrix(lengthFirst - 1)(lengthSecond - 1).toDouble) / maxLength
  }

  def checkEmailValidity(emailAddress: String): String = {
    //    Using General Email Regex (RFC 5322 Official Standard)
    if (emailAddress == null) return null
    val isValidEmail = emailAddress.matches("(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])")
    if (isValidEmail) emailAddress else null
  }

  def fillLastNameOnlyWhenFirstEqualsLastName(firstName: String, lastName: String, isFirstName:Boolean): String = {
    val firstNameWithoutNull = if (firstName == null) "" else firstName
    val lastNameWithoutNull = if (lastName == null) "" else lastName
    val isBothEqual = firstNameWithoutNull.equals(lastNameWithoutNull)
    val isNotEmptyFirst = firstNameWithoutNull != ""
    if(isFirstName) {
      if(isNotEmptyFirst && isBothEqual) "" else firstNameWithoutNull
    } else {
      lastNameWithoutNull
    }
  }

  def cleanPhoneNumber(phoneNumber: String, countryCode: String, countryPrefixList: Array[(String, String)]): String = {
    if (phoneNumber == null || countryCode == null) return null
    if (phoneNumber == "" || countryCode == "") return null
    val digitOnlyNumber = phoneNumber.replaceAll("(^0+)|[^0-9]+", "")
    if (digitOnlyNumber == "") return null
    val prefix = countryPrefixList.filter(_._1 == countryCode).map(_._2).head
    if (!digitOnlyNumber.startsWith(prefix)) prefix.concat(digitOnlyNumber) else digitOnlyNumber
  }

  def concatNames(firstName: String, lastName: String, emailAddress: String = ""): String = {
    try {
      val emailWithoutNull = if (emailAddress == null || emailAddress == "") "" else emailAddress
      val firstNameWithoutNull = if (firstName == null) "" else firstName
      val lastNameWithoutNull = if (lastName == null) "" else lastName
      if (firstNameWithoutNull == "" && lastName == "" && emailWithoutNull == "") return null
      if (firstNameWithoutNull == "" && emailWithoutNull == "") return lastName
      if (lastNameWithoutNull == "" && emailWithoutNull == "") return firstName

      val extractedNames = if (emailWithoutNull != "") emailWithoutNull.substring(0, emailWithoutNull.indexOf('@')) else ""
      if (firstNameWithoutNull == "" && lastNameWithoutNull == "" && extractedNames != "") {
        extractedNames match {
          case value: String if value.contains(".") => return extractedNames.split("\\.").sorted.mkString
          case value: String if value.contains("-") => return extractedNames.split("-").sorted.mkString
          case value: String if value.contains("_") => return extractedNames.split("_").sorted.mkString
          case _ => return extractedNames
        }

      }

      if (firstName.equals(lastName)) return firstName
      if (firstName < lastName) firstName + lastName else lastName + firstName
    } catch {
      case _: Exception => throw new Exception("string: ".concat(new String(firstName + ", " + lastName + ", " + emailAddress)))
    }
  }

  def getFastSimilarity(first: Array[Char], second: Array[Char]): Double = {
    if (first == null || second == null) return 0.0
    if (first.sameElements(second)) return 1.0

    val staysFirstString = if (first.length == second.length) true else if (first.length < second.length) true else false
    var firstString: Array[Char] = null
    var secondString: Array[Char] = null
    if (staysFirstString) {
      firstString = first
      secondString = second
    } else {
      firstString = second
      secondString = first
    }

    val lengthFirstString = firstString.length
    val lengthSecondString = secondString.length
    val lengthDifference = lengthSecondString - lengthFirstString

    if (lengthFirstString == 1) {
      lengthSecondString match {
        case 1.0 => return 0.0
        case _ => return 1.0 / lengthSecondString.toDouble
      }
    }
    var matchPairCounterFirstString: Double = 0.0
    var matchPairCounterSecondString: Double = 0.0
    val firstArray = new Array[Int](lengthFirstString - 1)
    val secondArray = new Array[Int](lengthSecondString - 1)
    for (i <- 1 until lengthFirstString) firstArray(i - 1) = (firstString(i - 1).toInt * firstString(i - 1).toInt) + (firstString(i).toInt * firstString(i).toInt)
    for (i <- 1 until lengthSecondString) secondArray(i - 1) = (secondString(i - 1).toInt * secondString(i - 1).toInt) + (secondString(i).toInt * secondString(i).toInt)
    for (i <- 0 until lengthFirstString.toInt - 1) {
      var duplicateCounterFirst = 0
      var duplicateCounterSecond = 0
      for (j <- 0 until lengthSecondString.toInt - 1) {
        if (j < lengthFirstString - 1 && firstArray(i) == firstArray(j)) {
          duplicateCounterFirst += 1
        }
        if (firstArray(i) == secondArray(j)) {
          matchPairCounterSecondString += 1.0
          duplicateCounterSecond += 1
        }
      }
      //      Handle duplicates in first and second string
      if (duplicateCounterSecond > 1) {
        matchPairCounterSecondString -= math.abs(duplicateCounterFirst - duplicateCounterSecond)
      }
      if (duplicateCounterFirst > 1) {
        if (duplicateCounterSecond > 1) {
          matchPairCounterSecondString -= (duplicateCounterFirst - 1)
        } else {
          matchPairCounterSecondString -= (1.0 - 1.0 / duplicateCounterFirst.toDouble)
        }
      }

    }
    matchPairCounterFirstString = lengthFirstString - 1

    val differenceCount = lengthDifference + (matchPairCounterFirstString - matchPairCounterSecondString)
    if (differenceCount > lengthFirstString) {
      0 /* Correct for small first and larger second string */
    } else if (differenceCount == 0) {
      (lengthFirstString - 1).toDouble / lengthFirstString.toDouble /* Correct inverse matches resulting in equality */
    } else {
      1 - differenceCount.toDouble / lengthFirstString.toDouble /* Taking length into account as well for final number */
    }
  }

  def getFileDateString:String = {
    val dateTime = java.time.LocalDateTime.now().toString
    s"${dateTime.substring(0, 4)}${dateTime.substring(5, 7)}${dateTime.substring(8, 10)}${dateTime.substring(11, 13)}${dateTime.substring(14, 16)}${dateTime.substring(17, 19)}"
  }

  def getNGrams(input: String, size: Int = 2): Array[String] = input.sliding(size).toArray

  def getSimilarity(first: String, second: String, setToLower: Boolean = false, lengthThreshold: Int = 6): Double = {
    if (first == null || second == null) return 0.0
    if (first == second) return 1.0

    val staysFirstString = if (first.length == second.length) first < second else if (first.length < second.length) true else false
    var firstString: String = null
    var secondString: String = null
    if (staysFirstString) {
      firstString = if (setToLower) first.toLowerCase() else first
      secondString = if (setToLower) second.toLowerCase() else second
    } else {
      firstString = if (setToLower) second.toLowerCase() else second
      secondString = if (setToLower) first.toLowerCase() else first
    }

    val lengthFirstString = firstString.length.toDouble
    val lengthSecondString = secondString.length.toDouble
    val lengthDifference = math.abs(lengthFirstString - lengthSecondString)
    if (lengthSecondString < lengthThreshold) return -1.0

    if (lengthFirstString == 1) {
      lengthSecondString match {
        case 1.0 => return 0.0
        case _ => return 1.0 / lengthSecondString
      }
    }

    try {
      var matchPairCounter: Double = 0.0
      var totalSinglesSameIndex: Double = 0.0
      for (i <- 0 until lengthFirstString.toInt) {
        val firstCharOne = firstString.charAt(i)
        val secondCharOne = if (i < lengthFirstString - 1) firstString.charAt(i + 1) else '‰'
        val sameIndexFirstCharTwo = secondString.charAt(i)
        if (firstCharOne == sameIndexFirstCharTwo) totalSinglesSameIndex += 1.0

        var isCountedPair = false
        for (j <- 0 until lengthSecondString.toInt) {
          val firstCharTwo = secondString.charAt(j)
          val secondCharTwo = if (j < lengthSecondString.toInt - 1) secondString.charAt(j + 1) else null
          val isDoubleMatch = firstCharOne == firstCharTwo && secondCharOne == secondCharTwo
          if (isDoubleMatch && !isCountedPair) {
            matchPairCounter += 1.0
            isCountedPair = true
          }
        }
      }

      //      Calculate how many times Similarity must by multiplied with itself to correct for big string comparisons
      val CORRECTION: Int = 30
      val POWER: Int = 1 + ((lengthFirstString.toInt - (lengthFirstString.toInt / CORRECTION)) / CORRECTION)
      val pairDenominator = if (lengthFirstString == lengthSecondString) lengthDifference + lengthFirstString else lengthDifference + lengthFirstString - 1

      def calculateSimilarityToPower(similarity: Double, input: Int = POWER): Double = {
        Math.pow(similarity, input)
      }

      calculateSimilarityToPower(math.max(matchPairCounter / pairDenominator, totalSinglesSameIndex / (lengthFirstString + lengthDifference)))
    } catch {
      case _: Exception => throw new Exception("first: ".concat(new String(firstString)).concat("; second: ").concat(new String(secondString)))
    }
  }

  def removeGenericStrangeChars(input: String): String = {
    input.replaceAll("[\u0021\u0023\u0025\u0026\u0028\u0029\u002A\u002B\u002D\u002F\u003A\u003B\u003C\u003D\u003E\u003F\u0040\u005E\u007C\u007E\u00A8\u00A9\u00AA\u00AC\u00AD\u00AF\u00B0\u00B1\u00B2\u00B3\u00B6\u00B8\u00B9\u00BA\u00BB\u00BC\u00BD\u00BE\u2013\u2014\u2022\u2026\u20AC\u2121\u2122\u2196\u2197\u247F\u250A\u2543\u2605\u2606\u3001\u3002\u300C\u300D\u300E\u300F\u3010\u3011\uFE36\uFF01\uFF06\uFF08\uFF09\uFF1A\uFF1B\uFF1F\u007B\u007D\u00AE\u00F7\u1BFC\u1BFD\u2260\u2264\u2DE2\u2DF2\uEC66\uEC7C\uEC7E\uED2B\uED34\uED3A\uEDAB\uEDFC\uEE3B\uEEA3\uEF61\uEFA2\uEFB0\uEFB5\uEFEA\uEFED\uFDAB\uFFB7\u007F\u24D2\u2560\u2623\u263A\u2661\u2665\u266A\u2764\uE2B1\uFF0D˱˳˵˶˹˻˼˽]+", "").trim
  }

  def removeSpacesNumbersStrangeCharsAndToLower(input: String): String = {
    input.toLowerCase().replaceAll("[ \u0024\u00A2\u00A3\u00A4\u00A5\u058F\u060B\u09F2\u09F3\u09FB\u0AF1\u0BF9\u0E3F\u17DB\u20A0\u20A1\u20A2\u20A3\u20A4\u20A5\u20A6\u20A7\u20A8\u20A9\u20AA\u20AB\u20AC\u20AD\u20AE\u20AF\u20B0\u20B1\u20B2\u20B3\u20B4\u20B5\u20B6\u20B7\u20B8\u20B9\u20BA\u20BB\u20BC\u20BD\u20BE\uA838\uFDFC\uFE69\uFF04\uFFE0\uFFE1\uFFE5\uFFE6\u0081\u0081°”\\\\_\\'\\~`!@#$%()={}|:;\\?/<>,\\.\\[\\]\\+\\-\\*\\^&:0-9]+", "")
  }

  def removeSpacesStrangeCharsAndToLower(input: String): String = {
    input.toLowerCase().replaceAll("[ \u0024\u00A2\u00A3\u00A4\u00A5\u058F\u060B\u09F2\u09F3\u09FB\u0AF1\u0BF9\u0E3F\u17DB\u20A0\u20A1\u20A2\u20A3\u20A4\u20A5\u20A6\u20A7\u20A8\u20A9\u20AA\u20AB\u20AC\u20AD\u20AE\u20AF\u20B0\u20B1\u20B2\u20B3\u20B4\u20B5\u20B6\u20B7\u20B8\u20B9\u20BA\u20BB\u20BC\u20BD\u20BE\uA838\uFDFC\uFE69\uFF04\uFFE0\uFFE1\uFFE5\uFFE6\u0081°”\\\\_\\'\\~`!@#$%()={}|:;\\?/<>,\\.\\[\\]\\+\\-\\*\\^&:]+", "")
  }

  def removeStrangeCharsToLowerAndTrim(input: String): String = {
    input.toLowerCase().replaceAll("(^\\s+)|(\\s+$)|[\u0024\u00A2\u00A3\u00A4\u00A5\u058F\u060B\u09F2\u09F3\u09FB\u0AF1\u0BF9\u0E3F\u17DB\u20A0\u20A1\u20A2\u20A3\u20A4\u20A5\u20A6\u20A7\u20A8\u20A9\u20AA\u20AB\u20AC\u20AD\u20AE\u20AF\u20B0\u20B1\u20B2\u20B3\u20B4\u20B5\u20B6\u20B7\u20B8\u20B9\u20BA\u20BB\u20BC\u20BD\u20BE\uA838\uFDFC\uFE69\uFF04\uFFE0\uFFE1\uFFE5\uFFE6\u0081°”\\\\_\\'\\~`!@#%()={}|:;\\?/<>,\\.\\[\\]\\+\\-\\*\\^&:]+", "").trim
  }
}
