import json
import os
from collections import defaultdict
from itertools import product
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# 初始化路径和参数
input_dir = '/lc05/ruandandan/datacourse/hupingzuoye2/'
output_dir = '/lc05/ruandandan/datacourse/hupingzuoye2/visualizations2/'
os.makedirs(output_dir, exist_ok=True)

# 中英对照映射
category_mapping = {
    '电子产品': 'Electronics',
    '汽车用品': 'Automotive',
    '服装用品': 'Apparel',
    '食品': 'Food',
    '家居用品': 'Home',
    '母婴用品': 'Baby',
    '办公用品': 'Office',
    '运动用品': 'Sports'
}

payment_mapping = {
    '现金': 'Cash',
    '支付宝': 'Alipay',
    '信用卡': 'CreditCard',
    '银联': 'UnionPay',
    '储蓄卡': 'DebitCard',
    '微信支付': 'WeChatPay',
    '云闪付': 'CloudPay'
}

refund_mapping = {
    '已退款': 'FullRefund',
    '部分退款': 'PartialRefund'
}

# 定义所有分类和支付方式（排序列表）
categories = sorted(category_mapping.values())
payments = sorted(payment_mapping.values())
refund_statuses = sorted(refund_mapping.values())

# 初始化统计数据结构
# 任务1: 商品关联分析
category_pairs = defaultdict(int)
category_counts = defaultdict(int)
total_orders = 0

# 任务2: 支付方式分析
payment_category = defaultdict(int)
payment_counts = defaultdict(int)
high_value_payments = defaultdict(int)

# 任务3: 时间序列分析
quarter_category = defaultdict(lambda: defaultdict(int))
quarter_total = defaultdict(int)

# 任务4: 退款分析
refund_category = {rs: defaultdict(int) for rs in refund_statuses}
refund_total = {rs: 0 for rs in refund_statuses}

# 处理数据文件
for i in range(16):
    filename = f'processed_purchases_{i}.jsonl'
    filepath = os.path.join(input_dir, filename)
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            order = json.loads(line)
            total_orders += 1
            
            # 转换商品大类
            majors = list({category_mapping[item['major_category']] 
                          for item in order['items']})
            
            # 任务1统计
            for a, b in product(majors, repeat=2):
                category_pairs[(a, b)] += 1
            for c in majors:
                category_counts[c] += 1
                
            # 转换支付方式
            payment = payment_mapping[order['payment_method']]
            
            for item in order['items']:
                # 任务2统计
                pc_key = (payment, category_mapping[item['major_category']])
                payment_category[pc_key] += 1
                payment_counts[payment] += 1
                
                # 高价值商品统计
                if item['price'] > 5000:
                    high_value_payments[payment] += 1
                
                # 任务3统计
                date = datetime.strptime(order['purchase_date'], '%Y-%m-%d')
                quarter = (date.month-1)//3 + 1
                category = category_mapping[item['major_category']]
                quarter_category[quarter][category] += 1
                quarter_total[quarter] += 1
                
                # 任务4统计
                status = refund_mapping.get(order['payment_status'], None)
                if status:
                    refund_category[status][category] += 1
                    refund_total[status] += 1

# ==================== 通用函数 ====================
def save_to_csv(results, filename):
    df = pd.DataFrame(results)
    df.to_csv(os.path.join(output_dir, filename), index=False)

# ==================== 任务1 可视化 ====================
# 支持度矩阵
support_matrix = pd.DataFrame(0.0, index=categories, columns=categories)
for (a, b), cnt in category_pairs.items():
    if a in categories and b in categories:
        support_matrix.loc[a, b] = cnt / total_orders

plt.figure(figsize=(15, 12))
sns.heatmap(support_matrix, annot=True, fmt=".2%", cmap="YlGnBu")
plt.title("Task1: Category Support Matrix")
plt.savefig(os.path.join(output_dir, 'task1_support_matrix.png'))
plt.close()

# 置信度矩阵
confidence_matrix = pd.DataFrame(0.0, index=categories, columns=categories)
for (a, b), cnt in category_pairs.items():
    if a in categories and b in categories and category_counts[a] > 0:
        confidence_matrix.loc[a, b] = cnt / category_counts[a]

plt.figure(figsize=(15, 12))
sns.heatmap(confidence_matrix, annot=True, fmt=".2f", cmap="YlGnBu")
plt.title("Task1: Category Confidence Matrix")
plt.savefig(os.path.join(output_dir, 'task1_confidence_matrix.png'))
plt.close()

# 输出符合规则的组合
task1_results = []
for (a, b) in product(categories, repeat=2):
    support = support_matrix.loc[a, b]
    confidence = confidence_matrix.loc[a, b]
    if support >= 0.02 and confidence >= 0.5:
        task1_results.append({
            'Antecedent': a,
            'Consequent': b,
            'Support': support,
            'Confidence': confidence
        })
save_to_csv(task1_results, 'task1_rules.csv')

# 电子产品关联气泡图
electronics_rules = []
for b in categories:
    if b == 'Electronics':
        continue
    support = support_matrix.loc['Electronics', b]
    confidence = confidence_matrix.loc['Electronics', b]
    electronics_rules.append({'Category': b, 'Support': support, 'Confidence': confidence})

df_electronics = pd.DataFrame(electronics_rules)
plt.figure(figsize=(10, 8))
sns.scatterplot(data=df_electronics, x='Support', y='Confidence', 
               size='Support', sizes=(100, 1000), hue='Category', palette='tab20')
plt.title("Task1: Electronics Association Rules")
plt.savefig(os.path.join(output_dir, 'task1_electronics_bubble.png'))
plt.close()

# ==================== 任务2 可视化 ====================
# 支付方式-商品支持度矩阵
payment_support = pd.DataFrame(0.0, index=payments, columns=categories)
for (p, c), cnt in payment_category.items():
    payment_support.loc[p, c] = cnt / total_orders

plt.figure(figsize=(15, 10))
sns.heatmap(payment_support, annot=True, fmt=".2%", cmap="YlGnBu")
plt.title("Task2: Payment-Category Support Matrix")
plt.savefig(os.path.join(output_dir, 'task2_payment_support.png'))
plt.close()

# 支付方式-商品置信度矩阵
payment_confidence = pd.DataFrame(0.0, index=payments, columns=categories)
for (p, c), cnt in payment_category.items():
    if payment_counts[p] > 0:
        payment_confidence.loc[p, c] = cnt / payment_counts[p]

plt.figure(figsize=(15, 10))
sns.heatmap(payment_confidence, annot=True, fmt=".2f", cmap="YlGnBu")
plt.title("Task2: Payment-Category Confidence Matrix")
plt.savefig(os.path.join(output_dir, 'task2_payment_confidence.png'))
plt.close()

# 输出符合规则的组合
task2_results = []
for (p, c) in product(payments, categories):
    support = payment_support.loc[p, c]
    confidence = payment_confidence.loc[p, c]
    if support >= 0.01 and confidence >= 0.6:
        task2_results.append({
            'Payment': p,
            'Category': c,
            'Support': support,
            'Confidence': confidence
        })
save_to_csv(task2_results, 'task2_rules.csv')

# 高价值支付方式分布
plt.figure(figsize=(10, 6))
ax = sns.barplot(x=list(high_value_payments.keys()), y=list(high_value_payments.values()))
plt.title("Task2: High-Value Payment Distribution")
for p in ax.patches:
    ax.annotate(f'{int(p.get_height())}', 
                (p.get_x() + p.get_width() / 2., p.get_height()),
                ha='center', va='center', 
                xytext=(0, 9), 
                textcoords='offset points')
plt.savefig(os.path.join(output_dir, 'task2_high_value.png'))
plt.close()

# 输出首选支付方式
max_payment = max(high_value_payments, key=lambda k: high_value_payments[k])
print(f"High-value preferred payment: {max_payment}")

# ==================== 任务3 可视化 ====================
# 季度购买趋势
quarter_data = []
for q in range(1, 5):
    for c in categories:
        count = quarter_category[q].get(c, 0)
        quarter_data.append({'Quarter': q, 'Category': c, 'Count': count})

df_quarter = pd.DataFrame(quarter_data)
plt.figure(figsize=(12, 8))
sns.lineplot(data=df_quarter, x='Quarter', y='Count', hue='Category', 
            marker='o', palette='tab20', linewidth=2.5)
plt.title("Task3: Seasonal Purchase Trends")
plt.savefig(os.path.join(output_dir, 'task3_seasonal.png'))
plt.close()

# ==================== 任务4 可视化 ====================
# 退款支持度矩阵
refund_support = pd.DataFrame(0.0, index=refund_statuses, columns=categories)
for status in refund_statuses:
    for c in categories:
        refund_support.loc[status, c] = refund_category[status].get(c, 0) / total_orders

plt.figure(figsize=(12, 6))
sns.heatmap(refund_support, annot=True, fmt=".4f", cmap="YlGnBu")
plt.title("Task4: Refund Support Matrix")
plt.savefig(os.path.join(output_dir, 'task4_refund_support.png'))
plt.close()

# 退款置信度矩阵
refund_confidence = pd.DataFrame(0.0, index=refund_statuses, columns=categories)
for status in refund_statuses:
    total = refund_total[status]
    for c in categories:
        if total > 0:
            refund_confidence.loc[status, c] = refund_category[status].get(c, 0) / total

plt.figure(figsize=(12, 6))
sns.heatmap(refund_confidence, annot=True, fmt=".2f", cmap="YlGnBu")
plt.title("Task4: Refund Confidence Matrix")
plt.savefig(os.path.join(output_dir, 'task4_refund_confidence.png'))
plt.close()

# 输出符合规则的组合
task4_results = []
for (s, c) in product(refund_statuses, categories):
    support = refund_support.loc[s, c]
    confidence = refund_confidence.loc[s, c]
    if support >= 0.005 and confidence >= 0.4:
        task4_results.append({
            'Status': s,
            'Category': c,
            'Support': support,
            'Confidence': confidence
        })
save_to_csv(task4_results, 'task4_rules.csv')