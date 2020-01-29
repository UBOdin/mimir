package mimir.util

import org.specs2.specification._
import org.specs2.mutable._

import org.spark_project.guava.reflect.ClassPath
import org.clapper.classutil.ClassFinder
import java.io.File


object SparkUtilsSpec
  extends Specification
{
  
  "SparkUtils" should {

    "Have the correct list of classes for Kryo" >> {
      val finder = ClassFinder(List(new File(".")))
      val classes = finder.getClasses  // classes is an Iterator[ClassInfo]
      val classMap = ClassFinder.classInfoMap(classes) // runs iterator out, once
      val models = ClassFinder.concreteSubclasses("mimir.models.Model", classMap).map(clazz => Class.forName(clazz.name)).toSeq
      val operators = ClassFinder.concreteSubclasses("mimir.algebra.Operator", classMap).map(clazz => Class.forName(clazz.name)).toSeq
      val expressions = ClassFinder.concreteSubclasses("mimir.algebra.Expression", classMap).map(clazz => Class.forName(clazz.name)).toSeq
      val samplers = ClassFinder.concreteSubclasses("mimir.algebra.sampling.SamplingMode", classMap).map(clazz => Class.forName(clazz.name)).toSeq
      val kryoClasses = (models ++ operators ++ expressions ++ samplers)
      SparkUtils.getSparkKryoClasses().toSeq must containTheSameElementsAs(kryoClasses)
    }

  }

}