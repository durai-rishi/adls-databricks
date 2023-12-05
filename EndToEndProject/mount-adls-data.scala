// Databricks notebook source


dbutils.fs.mount(
	source = "wasbs://bronze@onpremsqlstorage.blob.core.windows.net/",
	mountPoint = "/mnt/bronze",
	extraConfigs = Map("fs.azure.account.key.onpremsqlstorage.blob.core.windows.net"-> dbutils.secrets.get("testscope","adlsblobkey"))
	)


// COMMAND ----------


dbutils.fs.mount(
	source = "wasbs://silver@onpremsqlstorage.blob.core.windows.net/",
	mountPoint = "/mnt/silver",
	extraConfigs = Map("fs.azure.account.key.onpremsqlstorage.blob.core.windows.net"-> dbutils.secrets.get("testscope","adlsblobkey"))
	)
  

// COMMAND ----------


dbutils.fs.mount(
	source = "wasbs://gold@onpremsqlstorage.blob.core.windows.net/",
	mountPoint = "/mnt/gold",
	extraConfigs = Map("fs.azure.account.key.onpremsqlstorage.blob.core.windows.net"-> dbutils.secrets.get("testscope","adlsblobkey"))
	)



// COMMAND ----------


display(dbutils.fs.mounts())


