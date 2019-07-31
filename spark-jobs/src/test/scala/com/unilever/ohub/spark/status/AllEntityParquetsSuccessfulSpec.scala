package com.unilever.ohub.spark.status

import java.nio.file.{ Files, Path }
import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession._
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.domain.{ DomainEntityCompanion, DomainEntityUtils }

import scala.reflect.io.Directory

class AllEntityParquetsSuccessfulSpec extends SparkJobSpec {
  implicit val sparksession = spark
  val SUT = AllEntityParquetsSuccessful

  private def createSuccessFilesFor(entities: Seq[DomainEntityCompanion])(implicit runDir: Path): Unit = {
    Files.createDirectories(runDir)
    entities.map(_.engineFolderName).foreach((entity) ⇒ {
      val entityDir = runDir.resolve(s"${entity}.parquet")
      Files.createDirectory(entityDir)
      Files.createFile(entityDir.resolve("_SUCCESS"))
    })
  }

  private def removeFolder(folder: Path) = {
    new Directory(folder.toFile).deleteRecursively()
  }

  describe("AllEntityParquetsSuccessful") {
    it("Should fail if all entities are missing") {
      val runId = UUID.randomUUID().toString
      try {
        SUT.checkAllSuccessFiles(resourcesPath.toAbsolutePath.toString, runId)
      } catch {
        case _: NotAllEntitesSuccessfulException => succeed
        case e: ClassCastException => fail("Did you add a companionObject of type DomainEntityCompanion to new DomainEntities?")
        case e: Throwable ⇒ fail("Unexpected exception thrown", e)
      }
    }

    it("Should pass if all entities have a success file") {
      val runId = UUID.randomUUID().toString
      implicit val runDir = resourcesPath.resolve(runId)

      try {
        createSuccessFilesFor(DomainEntityUtils.getDomainCompanionObjects)
        SUT.checkAllSuccessFiles(resourcesPath.toAbsolutePath.toString, runId)
      } finally {
        removeFolder(runDir)
      }
    }

    it("Should fail if 1 entity is missing") {
      val runId = UUID.randomUUID().toString
      implicit val runDir = resourcesPath.resolve(runId)

      val excluded = ContactPerson
      val allButExcluded = DomainEntityUtils.getDomainCompanionObjects.filter(_ != excluded)

      try {
        createSuccessFilesFor(allButExcluded)
        SUT.checkAllSuccessFiles(resourcesPath.toAbsolutePath.toString, runId)
        fail("Exception must be thrown")
      } catch {
        case e: NotAllEntitesSuccessfulException ⇒ {
          e.getMessage should include(excluded.getClass.getSimpleName)
          allButExcluded.map(_.getClass.getSimpleName).foreach(e.getMessage should not include (_))
        }
        case e: Throwable ⇒ fail("Unexpected exception thrown", e)
      } finally {
        removeFolder(runDir)
      }
    }
  }
}
