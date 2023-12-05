// Databricks notebook source
import org.apache.spark.sql.functions._

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
// spark.conf.set("set spark.sql.legacy.timeParserPolicy=LEGACY")

// val df = spark.read.option("header",true).csv("/mnt/bronze/SalesLT/Address/Address.csv")

// val df1 = df.withColumn("ModifiedDate",to_date(df("ModifiedDate"),"yyyy-MM-dd"))

// display(df1)


// COMMAND ----------


import org.apache.spark.sql.functions._

var tblnamearray = Array.empty[String]
val tblnames = dbutils.fs.ls("/mnt/bronze/SalesLT")

for(i <- tblnames){
 
 tblnamearray :+= i.name.split("/")(0)
 
}

for(i <- tblnamearray){
val inputpath = "/mnt/bronze/SalesLT/"+i+"/"+i+".csv"
var df = spark.read.option("header",true).csv(inputpath)
val clm = df.columns

for(colm <- clm){

   if(clm.exists(_.contains("date")) || clm.exists(_.contains("Date"))){

    df = df.withColumn(colm,to_date(df(colm),"yyyy-MM-dd"))

  }

}
 
 val outputpath = "/mnt/silver/SalesLT/"+i+"/"

  df.write.format("delta").mode("overwrite").save(outputpath)


}




// COMMAND ----------

import org.apache.spark.sql.functions._

var df1 = spark.read.format("delta").load("/mnt/silver/SalesLT/Customer")

val clms = df1.columns

for(colm <- clms){

   if(clms.exists(_.toLowerCase.contains("date")) ){

    df1 = df1.withColumn(colm,to_date(df1(colm),"yyyy-MM-dd"))

  }

}

display(df1)

