from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType


def distributed_count(file, citation):
    spark = SparkSession.builder.master("local").getOrCreate()
    df = spark.read.options(header='True', delimiter='\t').csv(file)

    df = df.withColumn("citation_count", df["citation_count"].cast(IntegerType()))
    df = df.withColumn("paper_count", df["paper_count"].cast(IntegerType()))

    if citation:
        print("Order by citation count:")
        df.orderBy('citation_count', ascending=False).show()
    else:
        print("Order by paper count:")
        df.orderBy('paper_count', ascending=False).show()
    spark.stop()
