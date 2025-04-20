from pyspark.sql import SparkSession
import time
import pandas as pd
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

# 初始化Spark会话（优化配置）
spark = SparkSession.builder \
    .appName("MissingValueCleaning") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# 定义数据路径
input_path = "/lc05/ruandandan/datacourse/datamining/10G_data_new/*.parquet"

# 需要检查的字段列表
required_columns = [
    "id", 
    "age", 
    "income", 
    "gender", 
    "registration_date", 
    "last_login", 
    "purchase_history"
]

# ==================== 数据加载与初始统计 ====================
print("正在加载数据...")
start_time = time.time()

# 读取原始数据
df = spark.read.parquet(input_path)


original_count = df.count()
print(f"\n[初始状态] 总记录数：{original_count:,}")

# ==================== 缺失值统计 ====================
print("\n正在统计缺失值...")

# 计算每个字段的缺失数量和比例
missing_stats = []
for col_name in required_columns:
    missing_count = df.filter(df[col_name].isNull()).count()
    missing_ratio = missing_count / original_count
    missing_stats.append({
        "字段": col_name,
        "缺失数量": missing_count,
        "缺失比例": f"{missing_ratio:.2%}"
    })

# 转换为Pandas DataFrame展示
missing_df = pd.DataFrame(missing_stats)
print("\n[各字段缺失情况统计]")
print(missing_df.to_string(index=False))

# ==================== 删除缺失记录 ====================
print("\n正在删除缺失记录...")
clean_start = time.time()

# 删除指定字段存在缺失的记录
df_clean = df.dropna(subset=required_columns)
cleaned_count = df_clean.count()
removed_count = original_count - cleaned_count

# 计算处理时间
clean_time = time.time() - clean_start
total_time = time.time() - start_time

# ==================== 结果输出 ====================
print(f"\n[处理结果]")
print(f"原始记录数：{original_count:,}")
print(f"删除缺失记录数：{removed_count:,}")
print(f"剩余有效记录数：{cleaned_count:,}")
print(f"缺失记录占比：{removed_count/original_count:.2%}")
print(f"处理耗时：{clean_time//60:.0f}分{clean_time%60:.2f}秒")
print(f"总运行时间：{total_time//60:.0f}分{total_time%60:.2f}秒")

# 可选：保存清洗后的数据
# df_clean.write.parquet("path/to/cleaned_data")
