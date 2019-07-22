
// DATAFRAME PROJECT
// Use the Netflix_2011_2016.csv file to Answer and complete
// the commented tasks below!

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Load the Netflix Stock CSV File, have Spark infer the data types.
val dataFrame = spark.read.option("header","true").option("inferSchema","true").csv("Netflix_2011_2016.csv")

// What are the column names?
dataFrame.columns

// What does the Schema look like?
dataFrame.printSchema()

// Print out the first 5 columns.
dataFrame.head(5)

// Use describe() to learn about the DataFrame.
dataFrame.describe().show()
// Create a new dataframe with a column called HV Ratio that
// is the ratio of the High Price versus volume of stock traded
// for a day.
val dataFrame2 = dataFrame.withColumn("HV Ratio",dataFrame("High")/dataFrame("Volume"))

// What day had the Peak High in Price?
dataFrame.orderBy($"High".desc).show(1)

// What is the mean of the Close column?
dataFrame.select(mean("Close")).show()

// What is the max and min of the Volume column?
dataFrame.select(max("Volume")).show()
dataFrame.select(min("Volume")).show()
// For Scala/Spark $ Syntax

import spark.implicits._

// How many days was the Close lower than $ 600?
dataFrame.filter($"Close"<600).count()

// What percentage of the time was the High greater than $500 ?
dataFrame.filter($"High">500).count()*1.0/dataFrame.count()*100
// What is the Pearson correlation between High and Volume?
dataFrame.select(corr("High","Volume")).show()

// What is the max High per year?
val yeardf = dataFrame.withColumn("Year",year(dataFrame("Date")))
val yearMax = yeardf.select($"Year",$"High").groupBy("Year").max()
yearMax.select($"Year",$"max(High)").show()
// What is the average Close for each Calender Month?
val monthdf = dataFrame.withColumn("Month",month(dataFrame("Date")))
val monthAvgs = monthdf.select($"Month",$"Close").groupBy("Month").mean()
monthAvgs.select($"Month",$"avg(Close)").show()
