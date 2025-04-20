from pyspark.sql import SparkSession
import time
import pandas as pd
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
# 初始化Spark会话（优化大数据处理配置）
spark = SparkSession.builder\
    .appName("NullValueDetection")\
    .config("spark.driver.memory", "8g")\
    .config("spark.sql.shuffle.partitions", "200")\
    .getOrCreate()

# 定义数据路径和待检测字段
input_path = "/lc05/ruandandan/datacourse/datamining/30G_data_dropSame/*.parquet"
target_columns = [
    "id",
    "age",
    "income",
    "gender",
    "registration_date",
    "last_login",
    "purchase_history"
]

def validate_and_detect_nulls():
    """主函数：验证字段存在性并检测空值"""
    
    # === 数据加载 ===
    print("\n[1/4] 正在加载数据...")
    start_time = time.time()
    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"数据加载失败，请检查路径是否正确: {str(e)}")
        return

    # === 字段存在性验证 ===
    print("\n[2/4] 验证字段是否存在...")
    existing_columns = df.columns
    missing_columns = [col for col in target_columns if col not in existing_columns]
    
    if missing_columns:
        print(f"错误：以下字段不存在于数据集中: {missing_columns}")
        return
    else:
        print("所有目标字段验证通过")

    # === 空值检测 ===
    print("\n[3/4] 正在检测空值...")
    total_records = df.count()
    null_stats = []

    for col_name in target_columns:
        # 分布式计算空值数量
        null_count = df.filter(df[col_name].isNull()).count()
        null_ratio = null_count / total_records
        null_stats.append({
            "字段": col_name,
            "空值数量": null_count,
            "空值比例": f"{null_ratio:.4%}",
            "数据示例": df.select(col_name).take(3)  # 采样3条数据
        })

    # === 结果展示 ===
    print("\n[4/4] 检测结果报告")
    print(f"数据集总记录数: {total_records:,}")
    print("="*60)
    
    # 生成表格报告
    report_df = pd.DataFrame([{
        "字段": s["字段"],
        "空值数量": s["空值数量"],
        "空值比例": s["空值比例"]
    } for s in null_stats])
    
    print("\n[空值统计摘要]")
    print(report_df.to_string(index=False))
    
    # 详细诊断信息
    print("\n[字段级诊断]")
    for stat in null_stats:
        print(f"\n字段: {stat['字段']}")
        print(f"- 空值占比: {stat['空值比例']}")
        print("- 数据示例:")
        for row in stat["数据示例"]:
            val = row[0] if row[0] is not None else "NULL"
            print(f"  → {val}")

    # 性能统计
    elapsed = time.time() - start_time
    print(f"\n总耗时: {elapsed//60:.0f}分{elapsed%60:.2f}秒")

if __name__ == "__main__":
    validate_and_detect_nulls()