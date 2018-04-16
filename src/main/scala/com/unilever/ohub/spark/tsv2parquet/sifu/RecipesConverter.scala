package com.unilever.ohub.spark.tsv2parquet.sifu

import java.util.UUID

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.entity.{ Product, Recipe }
import com.unilever.ohub.spark.tsv2parquet.DomainTransformer
import org.apache.spark.sql.Row

object RecipesConverter extends SifuDomainGateKeeper[Recipe] {

  override protected[sifu] def sifuSelection: String = "recipes"

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Recipe = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

      // format: OFF

      val sourceName                                    =   "SIFU"
      val countryCode                                   =   "foo" // todo implement
      val sourceEntityId                                =   "foo" // todo implement
      val productId                                     =   UUID.randomUUID().toString
      val concatId                                      =   DomainEntity.createConcatIdFromValues(countryCode, sourceName, sourceEntityId)
      val ohubCreated                                   =   currentTimestamp()

     Recipe(
        // fieldName                  mandatory   sourceFieldName           targetFieldName                 transformationFunction (unsafe)
        concatId                        = concatId,
        countryCode                     = countryCode,
        customerType                    = Product.customerType,
        dateCreated                     = None,
        dateUpdated                     = None,
        isActive                        = true,
        isGoldenRecord                  = true,
        ohubId                          = Some(UUID.randomUUID().toString),
        name                            = mandatory( "foo",                 "name"),
        sourceEntityId                  = sourceEntityId,
        sourceName                      = sourceName,
        ohubCreated                     = ohubCreated,
        ohubUpdated                     = ohubCreated,
        // other fields
        additionalFields                = additionalFields,
        ingestionErrors                 = errors
      )
    // format: ON
  }

}
