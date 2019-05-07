// Databricks notebook source
// MAGIC %md
// MAGIC #### Q2 - Skeleton Scala Notebook
// MAGIC This template Scala Notebook is provided to provide a basic setup for reading in / writing out the graph file and help you get started with Scala.  Clicking 'Run All' above will execute all commands in the notebook and output a file 'examplegraph.csv'.  See assignment instructions on how to to retrieve this file. You may modify the notebook below the 'Cmd2' block as necessary.
// MAGIC 
// MAGIC #### Precedence of Instruction
// MAGIC The examples provided herein are intended to be more didactic in nature to get you up to speed w/ Scala.  However, should the HW assignment instructions diverge from the content in this notebook, by incident of revision or otherwise, the HW assignment instructions shall always take precedence.  Do not rely solely on the instructions within this notebook as the final authority of the requisite deliverables prior to submitting this assignment.  Usage of this notebook implicitly guarantees that you understand the risks of using this template code. 

// COMMAND ----------

/*
DO NOT MODIFY THIS BLOCK
This assignment can be completely accomplished with the following includes and case class.
Do not modify the %language prefixes, only use Scala code within this notebook.  The auto-grader will check for instances of <%some-other-lang>, e.g., %python
*/
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
case class edges(Source: String, Target: String, Weight: Int)
import spark.implicits._

// COMMAND ----------

/* 
Create an RDD of graph objects from our toygraph.csv file, convert it to a Dataframe
Replace the 'examplegraph.csv' below with the name of Q2 graph file.
*/

val df = spark.read.textFile("/FileStore/tables/bitcoinotc.csv") 
  .map(_.split(","))
  .map(columns => edges(columns(0), columns(1), columns(2).toInt)).toDF("source", "target", "weight")

// COMMAND ----------

// Eliminate duplicate rows and choose weight >= 5 rows
val df2 = df.distinct.filter("weight>=5")

// COMMAND ----------

// separate with corresponding in and out weight.
val source = df2.select("source", "weight").toDF("n", "w-out")
val target = df2.select("target", "weight").toDF("n", "w-in")

// group by nodes and calcualte sum of weights for source and target nodes
val source_df = source.groupBy("n").agg(sum("w-out"))
val target_df = target.groupBy("n").agg(sum("w-in"))

// join source and target nodes w/ weights together, fill null with 0, rename column names
val joined_df = source_df.join(target_df, Seq("n"), joinType="outer").select($"n".alias("node"), coalesce($"sum(w-out)", lit(0)).alias("weighted-out deg"), coalesce($"sum(w-in)",lit(0)).alias("weighted-in deg"))

// Add a new column of sum of in/out weighted degrees.
val totals = ($"weighted-out deg" + $"weighted-in deg")
val total_df = joined_df.withColumn("weighted total deg", totals)
total_df.show()


// COMMAND ----------

// find node with highest weighted-in-degree, if two or more nodes have the same weighted-in-degree, report the one with the lowest node id
val total_df_2 = total_df.select($"node", $"weighted-in deg".alias("w-in"))
val total_df_3 = total_df.select($"node", $"weighted-in deg".alias("w-in"))
val w_in_df = total_df_2.agg(max($"w-in").as("w-in")).join(total_df_3, Seq("w-in"),"inner").orderBy(asc("node")).select($"node", $"w-in")
val w_in_df2 = w_in_df.agg(min($"node").as("node")).join(w_in_df, Seq("node"), "inner").select($"node", $"w-in".as("w")).withColumn("c", lit("i"))
w_in_df2.show()

// COMMAND ----------

// find node with highest weighted-out-degree, if two or more nodes have the same weighted-out-degree, report the one with the lowest node id
val total_df_2 = total_df.select($"node", $"weighted-out deg".alias("w-out"))
val total_df_3 = total_df.select($"node", $"weighted-out deg".alias("w-out"))
val w_out_df = total_df_2.agg(max($"w-out").as("w-out")).join(total_df_3, Seq("w-out"),"inner").orderBy(asc("node")).select($"node", $"w-out")
val w_out_df2 = w_out_df.agg(min($"node").as("node")).join(w_out_df, Seq("node"), "inner").select($"node", $"w-out".as("w")).withColumn("c", lit("o"))
w_out_df2.show()

// COMMAND ----------

// find node with highest weighted-total degree, if two or more nodes have the same weighted-total-degree, report the one with the lowest node id
val total_df_2 = total_df.select($"node", $"weighted total deg".alias("w-total"))
val total_df_3 = total_df.select($"node", $"weighted total deg".alias("w-total"))
val w_total_df = total_df_2.agg(max($"w-total").as("w-total")).join(total_df_3, Seq("w-total"),"inner").orderBy(asc("node")).select($"node", $"w-total")
val w_total_df2 = w_total_df.agg(min($"node").as("node")).join(w_total_df, Seq("node"), "inner").select($"node", $"w-total".as("w")).withColumn("c", lit("t"))
w_total_df2.show()


// COMMAND ----------

/*
v,d,c
4,15,i
2,20,o
2,30,t
*/

val df_test = w_in_df2.union(w_out_df2)
val df_final = df_test.union(w_total_df2).select($"node" as "v", $"w" as "d", $"c")
display(df_final)

