import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Row, Column, DataFrame}
import com.google.common.collect.ImmutableMap
import org.apache.spark.rdd.RDD

val inputRDD1 = sc.textFile("file:///Users/aurobindosarkar/Downloads/MyBlogs/data/sNewsListWResults3yr/sNewsListWResults2012/*.txt")
val inputRDD2 = sc.textFile("file:///Users/aurobindosarkar/Downloads/MyBlogs/data/sNewsListWResults3yr/sNewsListWResults2013/*.txt")
val inputRDD3 = sc.textFile("file:///Users/aurobindosarkar/Downloads/MyBlogs/data/sNewsListWResults3yr/sNewsListWResults2014/*.txt")
val inputRDD = inputRDD1.union(inputRDD2).union(inputRDD3)

val inputCombinedRDD = sc.textFile("file:///Users/aurobindosarkar/Downloads/MyBlogs/data/sNewsListWResults3yr/combined.txt")

inputRDD.count()
inputCombinedRDD.count()

val inputDF = inputRDD.toDF()
val inputDropDupDF = inputDF.dropDuplicates()
inputDropDupDF.count()

val inputCombinedDF = inputCombinedRDD.toDF()
val inputCombinedDropDupDF = inputCombinedDF.dropDuplicates()
inputCombinedDropDupDF.count()

val intersectDF = inputCombinedDropDupDF.intersect(inputDropDupDF)
intersectDF.count()

val inputWithIDDF = inputDF.withColumn("keyID", monotonically_increasing_id())
inputWithIDDF.show()

inputDropDupDF.take(5).foreach(println)

def countSubstring( str:String, substr:String ) = substr.r.findAllMatchIn(str).length
def nSubs(substr: String) = udf((x: String) => countSubstring(x, substr))
val nCommasDF = inputDropDupDF.withColumn("commas", nSubs(",")($"value"))
nCommasDF.describe().select($"summary", $"commas").where(($"summary" === "count") || ($"summary" === "max") || ($"summary" === "min")).show()
nCommasDF.show()

def insertSubstring( str:String, substr:String, times: Int ): String = { val builder = StringBuilder.newBuilder; builder.append(str.substring(0, str.lastIndexOf(substr)+1));builder.append(substr * (22- times));builder.append(str.substring(str.lastIndexOf(substr)+1)); builder.toString;}
def nInserts(substr: String) = udf((x: String, times: Int) => insertSubstring(x, substr, times))

val fixedLengthDF = nCommasDF.withColumn("record", nInserts(",")($"value", $"commas"))
fixedLengthDF.groupBy("commas").count().sort($"commas".asc).show()
fixedLengthDF.groupBy("commas").count().sort($"count".desc).show()
fixedLengthDF.printSchema()

val fixedLengthDF = nCommasDF.withColumn("record", nInserts(",")($"value", $"commas")).filter($"commas" > 0).drop("value", "commas")
fixedLengthDF.take(5).foreach(println)
fixedLengthDF.count()

case class Portfolio(datestr: String, ls: String, stock1: String, stock2: String, stock3: String, stock4: String, stock5: String, stock6: String, stock7: String, stock8: String, stock9: String, stock10: String, stock11: String, stock12: String, stock13: String, stock14: String, stock15: String, stock16: String, stock17: String, stock18: String, stock19: String, stock20: String, avgstr: String )
val dfFixed = rowsRdd.map(s => Portfolio(s(0), s(1), s(2), s(3), s(4), s(5), s(6), s(7), s(8), s(9), s(10), s(11), s(12), s(13), s(14), s(15), s(16), s(17), s(18), s(19), s(20), s(21), s(22))).toDF()
dfFixed.count()
dfFixed.select("datestr", "ls", "stock1", "stock2", "avgstr").show(5)
val df2 = dfFixed.na.replace("stock2",ImmutableMap.of("", "NA")).select("datestr", "ls", "stock1", "stock2", "avgstr")
df2.show(5)
