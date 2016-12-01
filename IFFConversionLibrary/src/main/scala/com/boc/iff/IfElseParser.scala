package com.boc.iff

import java.io.FileInputStream

import scala.collection.mutable.Stack
import java.util.{Map, Properties}

import ognl.Ognl


object IfElseParser{
  val logger = new ECCLogger()
  val prop = new Properties()
  prop.load(new FileInputStream("/app/birdie/bochk/IFFConversion/config/config.properties"))
  logger.configure(prop)

  def parser(express:String,values:Map[String,Any]):Any={
    val expTrim = express.trim()
    if(expTrim.startsWith("IF")){
      var startIndex = expTrim.indexOf("(", 2)
      val condition = getSubExp(expTrim, startIndex, '(', ')')
      startIndex = expTrim.indexOf("{", startIndex+condition.length()+2)
      val trueExpr = getSubExp(expTrim, startIndex, '{', '}')
      startIndex += trueExpr.length()+2
      var falseExpr = ""
      if(expTrim.indexOf("ELSE",startIndex)>0){
        falseExpr = expTrim.substring(expTrim.indexOf("ELSE",startIndex)+4).trim()
        if(falseExpr.startsWith("{")){
          startIndex = expTrim.indexOf("ELSE",startIndex)+4
          falseExpr = getSubExp(expTrim, startIndex, '{', '}')
        }
      }else{
        falseExpr = "null"
      }
      if(Ognl.getValue(condition,values).asInstanceOf[Boolean]){
        parser(trueExpr, values)
      }else{
        parser(falseExpr, values)
      }
    }else{
     Ognl.getValue(expTrim, values)
    }
  }
  
  def getSubExp(express:String,startPos:Int,startChar:Char,endChar:Char):String={
    val s:Stack[Char] = new Stack[Char];
    var index = startPos
    if(express.charAt(index)!=startChar){
      null
    }else{
      s.push(express.charAt(index))
      index+=1
      while(!s.isEmpty && index<express.length()){
        if(express.charAt(index)==startChar){
          s.push(express.charAt(index))
        }else if(express.charAt(index)==endChar){
          s.pop()
        }
        index+=1
      }
      if(!s.isEmpty){
        null
      }else{
        express.substring(startPos+1,index-1);
      }
    }
  }
  
  
  
}