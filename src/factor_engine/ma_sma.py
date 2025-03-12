# ==== MA_Up_Lower.py ====
import pandas as pd
import matplotlib.pyplot as plt
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))  # 添加项目根目录到PYTHONPATH

# 修改为绝对导入路径
from src.data_engine.data_fetcher import DataFetcher
from src.data_engine.data_validator import DataValidator

class MA_SMA:
    """移动平均通道指标计算器"""
    
    def __init__(self, window=20, band_pct=0.03):
        """
        Args:
            window (int): 移动平均周期，默认20
            band_pct (float): 通道宽度比例，默认3%
        """
        self.window = window
        self.band_pct = band_pct
        
    def compute(self, df):
        """执行指标计算
        Args:
            df (DataFrame): 必须包含Close列
        Returns:
            DataFrame: 新增四列 [MA_Base, MA_UpperBand, MA_LowerBand, Band_Width]
        """
        df = df.copy()
        
        # 计算基准均线
        df['MA_Base'] = df['close'].rolling(window=self.window).mean()
        
        # 计算通道带
        df['MA_UpperBand'] = df['MA_Base'] * (1 + self.band_pct)
        df['MA_LowerBand'] = df['MA_Base'] * (1 - self.band_pct)
        
        # 计算通道宽度
        df['Band_Width'] = (df['MA_UpperBand'] - df['MA_LowerBand']) / df['MA_Base']
        
        return df.dropna()

# ==== 测试代码 ====
if __name__ == "__main__":
    # 初始化数据获取器
    fetcher = DataFetcher(
        symbol='588000',
        start_date='20240101',
        end_date='20250310'
    )

    # 修正：去掉 self，使用已初始化的 fetcher 实例
    price_df = fetcher.fetch_etf_data()
    
    # 初始化计算器
    calculator = MA_SMA(window=20, band_pct=0.02)
    
    # 执行计算
    band_df = calculator.compute(price_df)
    band_df.to_csv('MA_Up_Lower.csv')   
    
    # 结果分析
    print("\n移动平均通道统计摘要：")
    print(band_df[['MA_UpperBand', 'MA_LowerBand', 'Band_Width']].describe())
    
    # 修复中文显示问题
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    # 可视化片段（新增）
    plt.figure(figsize=(12, 6))
    plt.plot(band_df['close'], label='价格', alpha=0.5)
    plt.plot(band_df['MA_Base'], label=f'{calculator.window}日均线', linestyle='--')
    plt.fill_between(band_df.index,
                    band_df['MA_UpperBand'],
                    band_df['MA_LowerBand'],
                    color='skyblue', alpha=0.3)
    plt.title(f"{calculator.band_pct*100}% 移动平均通道")
    plt.xlabel("日期")
    plt.ylabel("价格")
    plt.legend()
    plt.show()
    print("指标数据样例：")
    print(band_df.tail(3))