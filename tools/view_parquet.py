"""
简单的 Parquet 文件查看工具
使用方法: python view_parquet.py <parquet_file_path>
"""

import sys
import pandas as pd

def view_parquet(file_path):
    """读取并显示 Parquet 文件的内容"""
    try:
        print(f"\n{'='*80}")
        print(f"正在读取: {file_path}")
        print(f"{'='*80}\n")
        
        # 读取 Parquet 文件
        df = pd.read_parquet(file_path)
        
        # 基本信息
        print(f"📊 数据维度: {df.shape[0]} 行 x {df.shape[1]} 列\n")
        
        # 列信息
        print(f"📋 列信息:")
        print(df.dtypes)
        print(f"\n{'='*80}\n")
        
        # 前几行数据
        print(f"👀 前 10 行数据:")
        print(df.head(10))
        print(f"\n{'='*80}\n")
        
        # 统计摘要
        print(f"📈 数值列统计:")
        print(df.describe())
        print(f"\n{'='*80}\n")
        
        # 缺失值统计
        print(f"❓ 缺失值统计:")
        missing = df.isnull().sum()
        missing_pct = (missing / len(df) * 100).round(2)
        missing_df = pd.DataFrame({
            '缺失数量': missing,
            '缺失比例(%)': missing_pct
        })
        print(missing_df[missing_df['缺失数量'] > 0])
        
        if missing.sum() == 0:
            print("✅ 没有缺失值！")
        
        print(f"\n{'='*80}\n")
        
        # 询问是否导出为 CSV
        export = input("是否导出为 CSV? (y/n): ").strip().lower()
        if export == 'y':
            csv_path = file_path.replace('.parquet', '.csv')
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f"✅ 已导出到: {csv_path}")
        
    except FileNotFoundError:
        print(f"❌ 错误: 文件不存在 - {file_path}")
    except Exception as e:
        print(f"❌ 错误: {str(e)}")
        print("\n💡 提示: 请确保安装了必要的库:")
        print("   pip install pandas pyarrow")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使用方法: python view_parquet.py <parquet_file_path>")
        print("示例: python view_parquet.py ./output/events.parquet")
    else:
        view_parquet(sys.argv[1])
