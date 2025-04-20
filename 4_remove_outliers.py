from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, to_date, when, get_json_object
from pyspark.sql.types import DoubleType, IntegerType
import time

# 初始化Spark会话（优化内存配置）
spark = SparkSession.builder \
    .appName("OptimizedOutlierHandling") \
    .config("spark.driver.memory", "12g") \
    .config("spark.sql.shuffle.partitions", "500") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
    .getOrCreate()

# 定义路径
input_path = "/lc05/ruandandan/datacourse/datamining/30G_data_new/*.parquet"
output_path = "/lc05/ruandandan/datacourse/datamining/30G_data_cleaned"

# 记录开始时间
start_time = time.time()

# ==================== 数据加载与解析 ====================
print("正在加载数据...")
df = spark.read.parquet(input_path)
original_count = df.count()
print(f"原始记录数：{original_count:,}")

# 轻量级JSON解析（仅提取必要字段）
df_parsed = df \
    .withColumn("purchase_avg_price", 
                get_json_object(col("purchase_history"), "$.avg_price").cast(DoubleType())) \
    .withColumn("login_count", 
                get_json_object(col("login_history"), "$.login_count").cast(IntegerType()))

# ==================== 异常检测 ====================
conditions = {
    "age异常": (col("age") < 0) | (col("age") > 120),
    "income异常": col("income") < 0,
    "avg_price异常": col("purchase_avg_price") < 0,
    "login_count异常": col("login_count") < 0,
    "注册时间异常": to_date(col("registration_date")) > to_date(col("last_login")),
    "电话格式异常": regexp_extract(col("phone_number"), "[a-zA-Z]", 0) != ""
}

# 分步统计异常数量（避免全量缓存）
abnormal_counts = {}
for name, cond in conditions.items():
    abnormal_counts[name] = df_parsed.filter(cond).count()
    print(f"{name}数量：{abnormal_counts[name]:,}")

# 合并异常条件
combined_cond = None
for cond in conditions.values():
    combined_cond = cond if combined_cond is None else combined_cond | cond

# ==================== 删除异常记录 ====================
print("正在删除异常记录...")
clean_start = time.time()
df_clean = df_parsed.filter(~combined_cond)
cleaned_count = df_clean.count()
removed_count = original_count - cleaned_count

# ==================== 结果输出 ====================
print("\n各类型异常统计：")
total_abnormal = sum(abnormal_counts.values())
for name, count in abnormal_counts.items():
    print(f"- {name}: {count:,} ({(count/original_count):.2%})")

print(f"\n总异常记录数：{total_abnormal:,}")
print(f"总异常记录占比：{total_abnormal/original_count:.2%}")
print(f"处理前记录数：{original_count:,}")
print(f"处理后记录数：{cleaned_count:,}")
print(f"删除记录数：{removed_count:,}")

# 保存结果（可选）
df_clean.write.mode("overwrite").parquet(output_path)

# 性能统计
total_time = time.time() - start_time
print(f"\n总运行时间：{total_time//60:.0f}分{total_time%60:.2f}秒")