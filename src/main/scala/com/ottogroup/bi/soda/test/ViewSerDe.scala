package com.ottogroup.bi.soda.test

import com.ottogroup.bi.soda.dsl.FieldLike
import scala.collection.mutable.ListBuffer
import com.ottogroup.bi.soda.dsl.Structure
import com.ottogroup.bi.soda.dsl.Field
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.FieldLike
import com.ottogroup.bi.soda.dsl.StructureDsl
import java.util.Date
import com.ottogroup.bi.soda.dsl.StructureDsl
import com.ottogroup.bi.soda.test.resources.LocalTestResources
import com.ottogroup.bi.soda.dsl.TextFile
import com.ottogroup.bi.soda.dsl.TextFile

object ViewSerDe {

  def serialize(v: View with rows): String = {
    v.storageFormat match {      
      case tf : TextFile => {
        val fterm = if (tf.fieldTerminator == null) "\t" else tf.fieldTerminator.replaceAll("\\\\t", "\t")
        val lterm = if (tf.lineTerminator == null) "\n" else tf.lineTerminator.replaceAll("\\\\n", "\n")
        v.rs.map(row =>
          v.fields.map(cell => {
            serializeCell(row(cell.n), false, tf)
          }).mkString(fterm))
        .mkString(lterm)
      }
      case _ => throw new RuntimeException("Can only serialize views stored as textfile")
    }
  }

  private def serializeCell(c: Any, inList: Boolean, format: TextFile): String = {
    c match {
      case null => { "\\N" }
      case s: Structure with values => { s.fields.map(f => serializeCell(s.fs(f.n), false, format)).mkString(if (inList) format.mapKeyTerminator else format.collectionItemTerminator) }
      case l: List[_] => { l.map(e => serializeCell(e, true, format)).mkString(format.collectionItemTerminator) }
      case m: Map[_, _] => { m.map(e => serializeCell(e._1, false, format) + format.mapKeyTerminator + serializeCell(e._2, false, format)).mkString(format.collectionItemTerminator) }
      case _ => { c.toString }
    }
  }

  def deserializeField[T](t: Manifest[T], v: String): Any = {
    //println("deserializing value " + v + " of type " + t)
    if (v == null || "null".equals(v)) {
      return v
    }
    if (t == manifest[Int])
      v.toInt
    else if (t == manifest[Long])
      v.toLong
    else if (t == manifest[Byte])
      v.toByte
    else if (t == manifest[Boolean])
      v.toBoolean
    else if (t == manifest[Double])
      v.toDouble
    else if (t == manifest[Float])
      v.toFloat
    else if (t == manifest[String])
      v
    else if (t == manifest[Date])
      v // TODO: parse date?
    else if (classOf[Structure].isAssignableFrom(t.runtimeClass)) {
      // Structures are given like [FieldValue1,FieldValue2,...]
      v.replaceAll("^\\[", "")
        .replaceAll("\\]$", "")
        .split(", ")
        .zip(t.runtimeClass.newInstance().asInstanceOf[Structure].fields)
        .map(el => (el._2, deserializeField(el._2.t, el._1)))
    } else if (t.runtimeClass == classOf[List[_]]) {
      // Lists are given like [el1, el2, ...]
      var delim = ", "
      if (classOf[Structure].isAssignableFrom(t.typeArguments.head.runtimeClass)) {
        delim = "\\], \\["
      }
      v.replaceAll("^\\[", "")
        .replaceAll("\\]$", "")
        .split(delim)
        .map(el => deserializeField(t.typeArguments.head, el))
        .toList
    } else if (t.runtimeClass == classOf[Map[_, _]]) {
      // Maps are given like {k1=v1, k2=v2, ...}
      v.replaceAll("^\\{", "")
        .replaceAll("\\}$", "")
        .split(", ")
        .map(el => el.split("="))
        .filter(a => a.size == 2)
        .map(a => (deserializeField(t.typeArguments(0), a(0)), deserializeField(t.typeArguments(1), a(1))))
        .toMap
    } else throw new RuntimeException("Could not deserialize field of type " + t + " with value " + v)
  }
}