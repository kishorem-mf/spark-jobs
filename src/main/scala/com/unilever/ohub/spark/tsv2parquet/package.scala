package com.unilever.ohub.spark

/**
  * tsv2parquet is responsible for INGESTING CSV files in Parquet files. The result is data that complies with the domain model.
  * There are several data sources that create CSV files and are represented as subpackages. Each package contain converters for a
  * specific data source like file_interface, emakina, fuzzit, sifu, web_event etc. A converter-package contains converters and the
  * converter logic for that specific data source. A converter contains validations, conversions and mapping between the domain
  * of the data source and the (master) data model of OHUB. The mapping logic and transformation logic ensures that a diverse set
  * of data sources, each having their own data and type specification will be normalized to a single model, and is represented
  * as a strongly typed Parquet file, which will be used for further processing.
  */
package object tsv2parquet {

}
