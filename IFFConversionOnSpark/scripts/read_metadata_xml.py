#!/usr/bin/python
# -*- coding: UTF-8 -*-

import sys
from xml.dom.minidom import parse
import xml.dom.minidom

xmlFile = sys.argv[1]
accountDateFieldName = ""
if len(sys.argv) == 3:
    accountDateFieldName = sys.argv[2]
DOMTree = xml.dom.minidom.parse(xmlFile)
collection = DOMTree.documentElement

beans = collection.getElementsByTagName("bean")

for bean in beans:
    if bean.hasAttribute("id"):
        beanId = bean.getAttribute("id")
        #print "beanId: %s" % beanId
        if beanId == "TargetSchema":
            constructorArg = bean.getElementsByTagName("constructor-arg")[0]
            value = constructorArg.getAttribute("value")
            print "%s" % value
        elif beanId == "TargetTable":
            constructorArg = bean.getElementsByTagName("constructor-arg")[0]
            value = constructorArg.getAttribute("value")
            print "%s" % value
        elif beanId == "body":
            fieldText = ""
            fields = bean.getElementsByTagName("bean")
            foundAccountDateField = False
            index = 0
            for field in fields:
                properties = field.getElementsByTagName("property")
                for property in properties:
                    propertyName = property.getAttribute("name")
                    if propertyName == "name":
                        propertyValue = property.getAttribute("value")
                        if accountDateFieldName != "" and propertyValue == accountDateFieldName:
                            foundAccountDateField = True
                        fieldText = fieldText + propertyValue
                        if index < len(fields) -1:
                            fieldText = fieldText + "\n"
                index += 1
            if accountDateFieldName != "" and foundAccountDateField == False:
                fieldText = "%s\n" % accountDateFieldName + fieldText
            print fieldText
