package com.boc.iff

import java.io.File

import org.apache.commons.lang3.StringUtils

import java.net.URLClassLoader

/**
  * Created by cvinc on 2016/7/28.
  */
class PasswordManager(classPath: String, className: String, method: String = "getPassword") {

  private val urls =
    StringUtils.split(classPath, ":").map { path =>
      if(path.startsWith("/")) path
      else System.getProperty("user.dir") + "/" + path
    }.map(new File(_)).filter(_.exists()).map(_.toURI.toURL)

  private val classLoader = new URLClassLoader(urls, this.getClass.getClassLoader)

  private val passwordUtilClass = classLoader.loadClass(className)

  private val passwordMethod = passwordUtilClass.getDeclaredMethod(method)

  val password = passwordMethod.invoke(null).asInstanceOf[String]

}
