package com.boc.iff.model

import java.util

import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions._


/**
  * Created by cvinc on 2016/6/8.
  */
class IFFSection extends Serializable {

  var fields: List[IFFField] = Nil

  def setFields(fields: util.ArrayList[IFFField]): Unit = {
    this.fields = fields.toList
  }

  def getLength: Int = if(fields.nonEmpty) fields.last.getEndPos else 0

  override def toString: String = {
    val sb = new StringBuilder()
    for(field<-fields){
      sb ++= field.toString + "\n"
    }
    sb.toString
  }

  def toXMLString: String = {
    val sb = new StringBuilder
    sb ++= "<bean class=\"" + this.getClass.getName + "\">"
    sb ++= "<property name=\"fields\"><util:list list-class=\"java.util.ArrayList\">"
    for (field <- fields) {
      sb ++= field.toXMLString
    }
    sb ++= "</util:list></property></bean>"
    sb.toString
  }

  def getSourceLength: Int = {
    fields.filter(x=>"Y".equals(x.virtual)).length
  }

}
