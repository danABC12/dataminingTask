from pyspark.sql import SparkSession
import time
import os

# 显式设置Java路径（根据实际环境调整）
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

# 初始化Spark会话（优化配置）
spark = SparkSession.builder.appName("Deduplicate_30G_Data")\
    .config("spark.driver.memory", "8g")\
    .config("spark.sql.shuffle.partitions", "200")\
    .getOrCreate()  

# 定义输入输出路径
input_path = "/lc05/ruandandan/datacourse/1G_data/*.parquet"
output_path = "/lc05/ruandandan/datacourse/1G_data_dropSame"

# 记录开始时间
start_time = time.time()

# 1. 读取原始数据
print("正在读取原始数据...")
df = spark.read.parquet(input_path)
original_count = df.count()
print(f"原始数据记录数：{original_count:,}")

# 2. 删除重复记录（根据所有列）
print("正在执行去重操作...")
df_deduplicated = df.dropDuplicates()

# 3. 保存去重后数据
print("正在写入去重后的数据...")
df_deduplicated.write \
    .mode("overwrite") \
    .parquet(output_path)

# 4. 统计结果
deduplicated_count = df_deduplicated.count()
removed_count = original_count - deduplicated_count

# 记录结束时间
end_time = time.time()
elapsed_time = end_time - start_time

# 打印结果报告
print("\n" + "="*50)
print(f"原始记录数：{original_count:,}")
print(f"去重后记录数：{deduplicated_count:,}")
print(f"删除重复记录数：{removed_count:,}")
print(f"重复记录占比：{removed_count/original_count:.2%}")
print(f"处理耗时：{elapsed_time//60:.0f}分{elapsed_time%60:.2f}秒")
print("="*50)