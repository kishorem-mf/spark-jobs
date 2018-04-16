package com.unilever.ohub.spark.tsv2parquet.fuzzit

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.tsv2parquet.DomainCsvGateKeeper

/**
 * Fuzzit data format,
 * (as taken from https:*confluence.oxyma.nl/display/UFS/ICD-02+Operator+and+Secondary+Sales+Data):
 *
 * Field delimiter: ;
 * EOL character: Carriage return/Line Feed (DOS)
 * Field encapsulation character: None
 * Header rows: None
 * Footer rows: None
 * Encoding: UTF-8
 * Filename: UFS_Fuzzit_YYYYMMDDHHMMSS.zip
 * Encrypted filename: Not applicable
 * Description: Zip file containing a full batch of data from the country.
 * The zip file contains several files described in the following sections.
 * Default max length: 256 characters
 */
trait FuzzitDomainGateKeeper[T <: DomainEntity] extends DomainCsvGateKeeper[T] {
  override final val fieldSeparator: String = ";"
  override final val hasHeaders = false
  override final val partitionByValue = "countryCode"
}
