package com.unilever.ohub.spark.tsv2parquet.sifu

import com.unilever.ohub.spark.SparkJobSpec
import io.circe
import io.circe.generic.auto._

class SifuDomainGateKeeperSpec extends SparkJobSpec {

  private val jsonString = """{"code":"25-NL-76129","number":"15871501","country":"nl","language":"NL","orderScore":"1","showGdaOnWebsite":"0","containerCode":"1304","containerName":"32,5 gr","itemType":"product","lastModifiedDate":"20180221092400","name":"Lipton Thee Professioneel Green Orient","brandCode":"597","brandName":"Lipton","subBrandCode":"969","subBrandName":"not defined","categoryCode":"1904","categoryName":"Thee","subCategoryCode":"4846","subCategoryName":"Lipton Thee Professioneel","packagingCode":"4110","packagingName":"2 x 3 x 25","properties":"Lipton Professioneel Green Tea Orient is een groene thee met een rustgevende werking. Deze groene thee valt in de 'Balance' categorie die helpt om een moment van balans te vinden gedurende de dag. De folie enveloppen bieden optimale bescherming voor de thee, hiermee blijft de geur en smaak goed behouden. ","preparation":null,"productiveness":null,"storage":null,"shelfLife":null,"tips":null,"benefits":null,"portionSize":"0","portionUnit":null,"cuEanCode":"5900300586974","duEanCode":"8722700587156","image1Id":"50292465","image1Text":null,"image2Id":"0","image2Text":null,"image3Id":"0","image3Text":null,"isUnileverProduct":"1","noteNutrients":null,"noteAddAllergen":null,"ingredients":"nb","nutrientRules":["NEP pink main dish"],"nutrientTypes":["energie kJ","energie kcal","vetten","vetten wv. verzadigde vetten","vetten wv. enkelvoudig onverzadigde vetten","vetten wv. meervoudig onverzadigde vetten","vetten wv. transvetten","koolhydraten","koolhydraten wv. suikers","voedingsvezels","eiwitten","natrium","kalium","vitamine A","vitamine D","Tocopherol, alpha","vitamine B6","vitamine B12","foliumzuur","zout"],"nutrientValues":["-  kJ","-  kcal","-  g","-  g","-  g","-  g","-  g","-  g","-  g","-  g","-  g","-  mg","-  mg","-  mcg_RE","-  mcg","-  mg","-  mg","-  mcg","-  mcg","-  g"],"nutrientPortionValues":["0 kJ","0 kcal","0.0 g","0.0 g","0.0 g","0.0 g","0.0 g","0.0 g","0.0 g","0.0 g","0.0 g","0 mg","0 mg","0 mcg_RE","0 mcg","0 mg","0 mg","0 mcg","0 mcg","0.0 g"],"additives":null,"dietetics":null,"allergens":null,"truthCopy":null,"truthVisualImageName":null,"solutionCopy":null,"mainAppVisualImageName":null,"rtbClaim1":null,"rtbClaim2":null,"rtbImageName":null,"rtbImageNameAfter":null,"youtubeUrl":null,"disclaimer1":null,"disclaimer2":null,"disclaimer3":null,"claimTitle":null,"claim1":null,"claim2":null,"claim3":null,"claim4":null,"madeIn":null,"howThisProductWillHelpYou":null,"slugifiedBrandName":"lipton","slugifiedCategoryName":"thee","slugifiedSubCategoryName":"lipton-thee-professioneel","cuPriceInCents":229,"duPriceInCents":1374,"cuLoyaltyPoints":2,"duLoyaltyPoints":14,"productType":"PRODUCT","mainAppVisualImageUrl":null,"packshotUrl":"https://sifu.unileversolutions.com/image/nl-NL/original/1/lipton-thee-professioneel-green-orient-50292465.jpg","packshotResizedUrl":"https://sifu.unileversolutions.com/image/nl-NL/original/1/200/200/lipton-thee-professioneel-green-orient-50292465.jpg","rtbImageAfterUrl":null,"rtbImageUrl":null,"truthVisualImageUrl":null,"nameSlug":"lipton-thee-professioneel-green-orient","availabilityHint":1,"showCuInfoHint":1,"showDuInfoHint":1,"url":null,"duOnlyProduct":false,"productNumber":"15871501","loyaltyReward":false,"productCode":"25-NL-76129","productCodes":["25-NL-76129"],"dataSourceId":"25-NL-76129","topProduct":false,"duAvailable":true,"rtbClaim":null,"cuAvailable":true,"available":true,"description":"Lipton Professioneel Green Tea Orient is een groene thee met een rustgevende werking. Deze groene thee valt in de 'Balance' categorie die helpt om een moment van balans te vinden gedurende de dag. De folie enveloppen bieden optimale bescherming voor de thee, hiermee blijft de geur en smaak goed behouden. ","defaultPackagingType":"BOTH"}"""

  describe("creating URI") {
    it("should return a valid URI") {
      val url = "https://sifu.unileversolutions.com:443/products/NL/nl/0/1?type=PRODUCT"
      val created = ProductConverter.createSifuURL("NL", "nl", "products", 0, 1)

      created.get.toString shouldBe url
    }
  }

  describe("decoding json string") {
    it("should be parsed into valid case class") {
      val json: Either[circe.Error, SifuProductResponse] = ProductConverter.decodeJsonString[SifuProductResponse](jsonString)

      json.isRight shouldBe true
      json.right.get.code.get shouldBe "25-NL-76129"
    }
  }

  describe("response body") {
    it("should return ") {
      val response = ProductConverter.getProductsFromApi("NL", "nl", "products", 100, 2)
      response.length shouldBe 200
    }
  }

}
