package com.unilever.ohub.spark.insights

case class DataCompletenessEntity(FILE_NAME: String,
                                  MODEL: String,
                                  SOURCE: String,
                                  TOTAL_ROW_COUNT: Long,
                                  DATA_FILLED_PERCENTAGE: Double,
                                  FPO_DATA_FILLED_PERCENTAGE: Double
                                 )
