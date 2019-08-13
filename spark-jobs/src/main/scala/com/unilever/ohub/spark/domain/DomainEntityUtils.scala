package com.unilever.ohub.spark.domain

import org.reflections.Reflections

import scala.reflect.runtime.universe._

object DomainEntityUtils {
  val mirror = runtimeMirror(getClass.getClassLoader)

  /**
    * Function uses reflection to look for all [[com.unilever.ohub.spark.domain.DomainEntity]] objects
    * (under [[com.unilever.ohub.spark.domain.DomainEntity]]'s package) and fetches the
    * corresponding [[com.unilever.ohub.spark.domain.DomainEntityCompanion]]s.
    *
    * @return
    */
  lazy val domainCompanionObjects: Array[DomainEntityCompanion[_ <: DomainEntity]] = {
    val reflections = new Reflections(classOf[DomainEntity].getPackage.getName)
    val types = reflections.getSubTypesOf[DomainEntity](classOf[DomainEntity])

    types.toArray
      .map(_.asInstanceOf[java.lang.Class[DomainEntity]])
      .map((domainEntityClass) ⇒ {
        val module = mirror.staticModule(domainEntityClass.getName)
        mirror.reflectModule(module).instance.asInstanceOf[DomainEntityCompanion[_ <: DomainEntity]]
      })
  }

  def domainCompanionOf[T](implicit tag: TypeTag[T]): DomainEntityCompanion[_ <: DomainEntity] = {
    val companionModule = tag.tpe.typeSymbol.companion.asModule
    mirror.reflectModule(companionModule).instance.asInstanceOf[DomainEntityCompanion[_ <: DomainEntity]]
  }
}
