package SparkPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.Row 
import org.apache.spark.sql.SparkSession 
import org.apache.spark.sql.types._ 

object ComplexJSON {
	def main (args:Array[String]):Unit={  
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]") 
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate() 
					import spark.implicits._ 
					val jsondf = spark.read.format("json").option("multiLine","true").load("file:///C://data//zeyodata.json")
					jsondf.show() 
					jsondf.printSchema()
					val selectdata = jsondf.select("No","Year","address.temporary_address","address.permanent_address","firstname" ,"lastname" ) 
					selectdata.show() 
					selectdata.printSchema() 
	}
}