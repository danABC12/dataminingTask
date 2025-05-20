import json
import os
import pandas as pd
from pandas import DataFrame
from typing import Generator, Dict, Any
import pyarrow.parquet as pq
# 配置参数
PARQUET_PATH = "/lc05/ruandandan/datacourse/datamining/part_15"
PRODUCT_CATALOG = "/lc05/ruandandan/datacourse/hupingzuoye2/product_catalog.json"
OUTPUT_JSONL = "/lc05/ruandandan/datacourse/hupingzuoye2/processed_purchases_15.jsonl"
CHUNK_SIZE = 50000  # 根据内存调整分块大小

# 自定义大类映射规则（需根据实际类别补充完整）

CATEGORY_MAPPING = {
    # 电子产品大类
    "智能手机": "电子产品",
    "平板电脑": "电子产品",
    "笔记本电脑": "电子产品",
    "游戏机": "电子产品",
    "智能手表": "电子产品",
    "耳机": "电子产品",
    "相机": "电子产品",
    "摄像机": "电子产品",
    "音响": "电子产品",
    

    # 汽车用品大类
    "汽车装饰": "汽车用品",
    "车载电子": "汽车用品",

    
    # 服装用品大类
    "上衣": "服装用品",
    "裤子": "服装用品",
    "裙子": "服装用品",
    "外套": "服装用品",
    "内衣": "服装用品",
    "帽子": "服装用品",
    "围巾": "服装用品",
    "手套": "服装用品",
    "鞋子": "服装用品",
    
    
    # 食品大类
    "零食": "食品",
    "饮料": "食品",
    "米面": "食品",
    "蛋奶": "食品",
    "水果": "食品",
    "蔬菜": "食品",
    "肉类": "食品",
    "水产": "食品",
    "调味品": "食品",
    

    # 家居用品
    "厨具": "家居用品",
    "卫浴用品": "家居用品",
    "床上用品": "家居用品",
    "家具": "家居用品",
    "模型": "家居用品", 
    

    # 母婴用品大类
    "玩具": "母婴用品",
    "益智玩具": "母婴用品",
    "婴儿用品": "母婴用品",
    "儿童课外读物": "母婴用品",


    # 办公用品大类
    "办公用品": "办公用品",
    "文具": "办公用品",
    "模型": "办公用品",


    # 运动用品大类
    "健身器材": "运动用品",
    "户外装备": "运动用品"
}


def load_product_catalog() -> Dict[int, Dict]:
    """加载商品目录并添加大类信息"""
    with open(PRODUCT_CATALOG, 'r', encoding='utf-8') as f:
        catalog = json.load(f)
    
    product_map = {}
    for product in catalog["products"]:
        major = CATEGORY_MAPPING.get(product["category"], "其他")
        product_map[product["id"]] = {
            "category": product["category"],
            "price": product["price"],
            "major_category": major
        }
    return product_map


def process_purchases(product_map: Dict) -> Generator[DataFrame, None, None]:
    """分块处理Parquet文件（兼容PyArrow）"""
    for root, _, files in os.walk(PARQUET_PATH):
        for file in files:
            if file.endswith(".parquet"):
                path = os.path.join(root, file)
                # 使用PyArrow直接分块读取
                parquet_file = pq.ParquetFile(path)
                for batch in parquet_file.iter_batches(batch_size=CHUNK_SIZE):
                    df = batch.to_pandas()
                    yield process_chunk(df, product_map)



def process_chunk(chunk: DataFrame, product_map: Dict) -> DataFrame:
    """处理单个数据块"""
    processed = []
    for _, row in chunk.iterrows():
        try:
            purchase = json.loads(row["purchase_history"])
            items = []
            for item in purchase["items"]:
                product_id = item["id"]
                if product_id in product_map:
                    item_data = product_map[product_id].copy()
                    item_data["id"] = product_id
                    items.append(item_data)
            
            if items:
                processed.append({
                    "items": items,
                    "payment_method": purchase.get("payment_method"),
                    "payment_status": purchase.get("payment_status"),
                    "purchase_date": purchase.get("purchase_date"),
                    "item_count": len(items)
                })
        except (json.JSONDecodeError, KeyError) as e:
            print(f"数据解析异常：{str(e)}")
            continue
    
    return pd.DataFrame(processed)

def main():
    # 步骤1：加载商品目录
    product_map = load_product_catalog()
    print(f"已加载 {len(product_map)} 个商品信息")
    
    # 步骤2：分块处理数据
    first_write = True
    for df_chunk in process_purchases(product_map):
        if not df_chunk.empty:
            df_chunk.to_json(
                OUTPUT_JSONL,
                orient="records",
                lines=True,
                force_ascii=False,
                mode="a" if not first_write else "w"
            )
            first_write = False
            print(f"已处理 {len(df_chunk)} 条订单记录")

if __name__ == "__main__":
    main()