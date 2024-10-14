from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

def process_drive_data(data_lake_path, daily_output_path, yearly_output_path):
    # 创建Spark会话
    spark = SparkSession.builder \
        .appName("Drive Data Processing") \
        .getOrCreate()

    # 创建输出目录
    os.makedirs(daily_output_path, exist_ok=True)
    os.makedirs(yearly_output_path, exist_ok=True)

    # 定义品牌名称和前缀的映射
    brand_prefixes = {
        "Crucial": "CT",
        "Dell BOSS": "DELLBOSS",
        "HGST": "HGST",
        "Seagate": ["Seagate", "ST"],
        "Toshiba": "TOSHIBA",
        "Western Digital": "WDC",
    }

    # 提取品牌名称
    def extract_brand(model):
        for brand, prefix in brand_prefixes.items():
            if isinstance(prefix, list):
                if any(model.startswith(p) for p in prefix):
                    return brand
            elif model.startswith(prefix):
                return brand
        return "Others"

    extract_brand_udf = F.udf(extract_brand)

    # 读取数据并合并所有CSV文件
    df = spark.read.option("header", "true").csv(data_lake_path + "*.csv")

    # 添加品牌列
    df = df.withColumn("brand", extract_brand_udf(df["model"]))

    # Daily summary
    daily_summary = df.groupBy("date", "brand").agg(
        F.count("*").alias("count"),
        F.sum(df["failure"].cast("int")).alias("failures")
    )

    # 将每日汇总结果写入CSV
    for brand in daily_summary.select("brand").distinct().rdd.flatMap(lambda x: x).collect():
        brand_summary = daily_summary.filter(daily_summary["brand"] == brand)
        brand_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(daily_output_path, f"{brand}.csv"))

    # Yearly summary
    df = df.withColumn("year", F.year("date"))
    yearly_summary = df.groupBy("year", "brand").agg(
        F.sum(df["failure"].cast("int")).alias("failures")
    )

    # 将年度汇总结果写入CSV
    for brand in yearly_summary.select("brand").distinct().rdd.flatMap(lambda x: x).collect():
        brand_yearly_summary = yearly_summary.filter(yearly_summary["brand"] == brand)
        brand_yearly_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(yearly_output_path, f"{brand}.csv"))

    spark.stop()
