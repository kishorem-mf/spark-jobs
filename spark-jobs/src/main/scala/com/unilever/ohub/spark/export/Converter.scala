package com.unilever.ohub.spark.export

import com.unilever.ohub.spark.domain.DomainEntity

trait Converter[DomainType <: DomainEntity, OutboundType <: OutboundEntity] {

  def convert(d: DomainType): OutboundType
}
