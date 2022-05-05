# hhh
lev Navigate Sode Analyze Befactor Build Run Iools VES Window Hep
sIG) main scala (- IsonParserscala
to rawjson
Examole/scala
fo raw son ison
Eg Soubon scala2


© DilG
def extractüsonString(in: Strirg) = {
Try (in. substring(in. indexofP-in_networkf°), Try(in.Indexo?(-]=)). getonELse(&n. length))).getonEtse (an)
val
jsonExtractUdf = spark.udf.register( name- "jsonExtractUdf", extractasonstring(_:Strang):String)
import spark.implicits..
val ramJson = spark.read.textFile( path = "raw. json").rdd.zipmithindexO. toDF( colName:
value"
. filter (!col ( colName = "index" ).isin(Seg(8,1,2,3,4,5,6,7,8):_*)). mithColumn( colName-
'acD
Mindex:)
Lit( deal a "1 )). cache()
val indexValues = ramJson.select(min(col( colName = "index")) , max(col ( colName-
.collect().map(row => (row.getLong(@), row.getLong(1))). toseg(e)

val test = ramJson
.groupBy(col(colName="grp")).agg(concat_ws(sep=
•select(concat_ws( sep = "', Lit( literal = "("), col( colName=
collect. List(trin(col(colName
*vatue®)) . as( alas=: "values))
evalue!
select( col= "value")
/*
.withcolumn("value",
when (col ("'index") === indexvalues._1,
concat_us("", Lit("("), col ("value")))
. when (col("index") === indexValues._2, concat.ws("", Lit("LASTIII*), coL("value"))).otherwise(col ("value ))).
test.snow( numRows= 500,
truncate = false)
val parse = raJson.withcolumn( colName=
"value"
when (col( colName = "index")
=== indexValves..
stepnee.
