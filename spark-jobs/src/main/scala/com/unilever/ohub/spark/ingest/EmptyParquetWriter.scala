package com.unilever.ohub.spark.ingest




import com.unilever.ohub.spark.domain.{DomainEntity}
import com.unilever.ohub.spark.domain.entity._
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}


trait EmptyParquetWriter[T <: DomainEntity] {
  def createEmptyDataset(spark: SparkSession): Dataset[T]

  def writeEmptyParquet(spark: SparkSession, storage: Storage, location: String): Unit = {
    val ds = createEmptyDataset(spark)

    storage.writeToParquet(ds, location, saveMode = SaveMode.Ignore) // prevent overwriting existing data
  }
}

trait ContactPersonEmptyParquetWriter extends EmptyParquetWriter[ContactPerson] {

  def createEmptyDataset(spark: SparkSession): Dataset[ContactPerson] = {
    import spark.implicits._

    spark.createDataset[ContactPerson](Seq[ContactPerson]())
  }
}

trait OperatorEmptyParquetWriter extends EmptyParquetWriter[Operator] {

  def createEmptyDataset(spark: SparkSession): Dataset[Operator] = {
    import spark.implicits._

    spark.createDataset[Operator](Seq[Operator]())
  }
}

trait ProductEmptyParquetWriter extends EmptyParquetWriter[Product] {

  def createEmptyDataset(spark: SparkSession): Dataset[Product] = {
    import spark.implicits._

    spark.createDataset[Product](Seq[Product]())
  }
}

trait OrderEmptyParquetWriter extends EmptyParquetWriter[Order] {

  def createEmptyDataset(spark: SparkSession): Dataset[Order] = {
    import spark.implicits._

    spark.createDataset[Order](Seq[Order]())
  }
}

trait OrderLineEmptyParquetWriter extends EmptyParquetWriter[OrderLine] {

  def createEmptyDataset(spark: SparkSession): Dataset[OrderLine] = {
    import spark.implicits._

    spark.createDataset[OrderLine](Seq[OrderLine]())
  }
}

trait SubscriptionEmptyParquetWriter extends EmptyParquetWriter[Subscription] {

  def createEmptyDataset(spark: SparkSession): Dataset[Subscription] = {
    import spark.implicits._

    spark.createDataset[Subscription](Seq[Subscription]())
  }
}

trait QuestionEmptyParquetWriter extends EmptyParquetWriter[Question] {

  def createEmptyDataset(spark: SparkSession): Dataset[Question] = {
    import spark.implicits._

    spark.createDataset[Question](Seq[Question]())
  }
}

trait ActivityEmptyParquetWriter extends EmptyParquetWriter[Activity] {

  def createEmptyDataset(spark: SparkSession): Dataset[Activity] = {
    import spark.implicits._

    spark.createDataset[Activity](Seq[Activity]())
  }
}

trait AnswerEmptyParquetWriter extends EmptyParquetWriter[Answer] {

  def createEmptyDataset(spark: SparkSession): Dataset[Answer] = {
    import spark.implicits._

    spark.createDataset[Answer](Seq[Answer]())
  }
}

trait AssetMovementEmptyParquetWriter extends EmptyParquetWriter[AssetMovement] {

  def createEmptyDataset(spark: SparkSession): Dataset[AssetMovement] = {
    import spark.implicits._

    spark.createDataset[AssetMovement](Seq[AssetMovement]())
  }
}

trait LoyaltyPointsEmptyParquetWriter extends EmptyParquetWriter[LoyaltyPoints] {

  def createEmptyDataset(spark: SparkSession): Dataset[LoyaltyPoints] = {
    import spark.implicits._

    spark.createDataset[LoyaltyPoints](Seq[LoyaltyPoints]())
  }
}

trait CampaignBounceEmptyParquetWriter extends EmptyParquetWriter[CampaignBounce] {

  def createEmptyDataset(spark: SparkSession): Dataset[CampaignBounce] = {
    import spark.implicits._

    spark.createDataset[CampaignBounce](Seq[CampaignBounce]())
  }
}

trait CampaignClickEmptyParquetWriter extends EmptyParquetWriter[CampaignClick] {

  def createEmptyDataset(spark: SparkSession): Dataset[CampaignClick] = {
    import spark.implicits._

    spark.createDataset[CampaignClick](Seq[CampaignClick]())
  }
}

trait CampaignOpenEmptyParquetWriter extends EmptyParquetWriter[CampaignOpen] {

  def createEmptyDataset(spark: SparkSession): Dataset[CampaignOpen] = {
    import spark.implicits._

    spark.createDataset[CampaignOpen](Seq[CampaignOpen]())
  }
}

trait CampaignSendEmptyParquetWriter extends EmptyParquetWriter[CampaignSend] {

  def createEmptyDataset(spark: SparkSession): Dataset[CampaignSend] = {
    import spark.implicits._

    spark.createDataset[CampaignSend](Seq[CampaignSend]())
  }
}

trait CampaignEmptyParquetWriter extends EmptyParquetWriter[Campaign] {

  def createEmptyDataset(spark: SparkSession): Dataset[Campaign] = {
    import spark.implicits._

    spark.createDataset[Campaign](Seq[Campaign]())
  }
}

trait ChannelMappingEmptyParquetWriter extends EmptyParquetWriter[ChannelMapping] {

  def createEmptyDataset(spark: SparkSession): Dataset[ChannelMapping] = {
    import spark.implicits._

    spark.createDataset[ChannelMapping](Seq[ChannelMapping]())
  }
}

trait ChainEmptyParquetWriter extends EmptyParquetWriter[Chain] {

  def createEmptyDataset(spark: SparkSession): Dataset[Chain] = {
    import spark.implicits._

    spark.createDataset[Chain](Seq[Chain]())
  }
}

trait OperatorChangeLogEmptyParquetWriter extends EmptyParquetWriter[OperatorChangeLog] {


  def createEmptyDataset(spark: SparkSession): Dataset[OperatorChangeLog] = {
    import spark.implicits._

    spark.createDataset[OperatorChangeLog](Seq[OperatorChangeLog]())

  }
}

trait ContactPersonChangeLogEmptyParquetWriter extends EmptyParquetWriter[ContactPersonChangeLog] {

  def createEmptyDataset(spark: SparkSession): Dataset[ContactPersonChangeLog] = {
    import spark.implicits._

    spark.createDataset[ContactPersonChangeLog](Seq[ContactPersonChangeLog]())
  }
}

trait ContactPersonGoldenEmptyParquetWriter extends EmptyParquetWriter[ContactPersonGolden] {

  def createEmptyDataset(spark: SparkSession): Dataset[ContactPersonGolden] = {
    import spark.implicits._

    spark.createDataset[ContactPersonGolden](Seq[ContactPersonGolden]())
  }
}

trait ContactPersonRexLiteEmptyParquetWriter extends EmptyParquetWriter[ContactPersonRexLite] {

  def createEmptyDataset(spark: SparkSession): Dataset[ContactPersonRexLite] = {
    import spark.implicits._

    spark.createDataset[ContactPersonRexLite](Seq[ContactPersonRexLite]())
  }
}

trait OperatorRexLiteEmptyParquetWriter extends EmptyParquetWriter[OperatorRexLite] {

  def createEmptyDataset(spark: SparkSession): Dataset[OperatorRexLite] = {
    import spark.implicits._

    spark.createDataset[OperatorRexLite](Seq[OperatorRexLite]())
  }
}

trait OperatorGoldenEmptyParquetWriter extends EmptyParquetWriter[OperatorGolden] {

  def createEmptyDataset(spark: SparkSession): Dataset[OperatorGolden] = {
    import spark.implicits._

    spark.createDataset[OperatorGolden](Seq[OperatorGolden]())
  }
}

trait AssetEmptyParquetWriter extends EmptyParquetWriter[Asset] {

  def createEmptyDataset(spark: SparkSession): Dataset[Asset] = {
    import spark.implicits._

    spark.createDataset[Asset](Seq[Asset]())
  }
}
