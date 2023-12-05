// Databricks notebook source

val silvertblnames = dbutils.fs.ls("/mnt/silver/SalesLT")

display(silvertblnames)


// COMMAND ----------



import org.apache.spark.sql.functions._

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

var tblnamearray = Array.empty[String]
val tblnames = dbutils.fs.ls("/mnt/silver/SalesLT")

for(i <- tblnames){
 
 tblnamearray :+= i.name.split("/")(0)
 
}

for(i <- tblnamearray){
val inputpath = "/mnt/silver/SalesLT/"+i
var df = spark.read.format("delta").load(inputpath)
val clm = df.columns

for(colm <- clm){

   if(clm.exists(_.contains("date")) || clm.exists(_.contains("Date"))){

    df = df.withColumn(colm,to_date(df(colm),"yyyy-MM-dd"))

  }

}
 
 val outputpath = "/mnt/gold/SalesLT/"+i+"/"

  df.write.format("delta").mode("overwrite").save(outputpath)


}




