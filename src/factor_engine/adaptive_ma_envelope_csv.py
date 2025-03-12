# ==== adaptive_ma_envelope.py ====
import pandas as pd
import numpy as np
import sys
import os
import matplotlib.pyplot as plt

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))  # 添加项目根目录到PYTHONPATH

# 修改为绝对导入路径
from src.data_engine.data_fetcher import DataFetcher
from src.data_engine.data_validator import DataValidator
from datetime import datetime

class AdaptiveMAEnvelope:
    """基于波动率的自适应移动平均通道"""
    
    def __init__(self, base_window=20, vol_window=20, scale_factor=2.0, clip_range=(0.01, 0.05)):
        """
        Args:
            base_window (int): 基础移动平均窗口，默认20天
            vol_window (int): 波动率计算窗口，默认20天
            scale_factor (float): 波动率缩放系数，默认2.0
            clip_range (tuple): 包络百分比限制范围，默认(1%,5%)
        """
        self.base_window = base_window
        self.vol_window = vol_window
        self.scale_factor = scale_factor
        self.clip_min, self.clip_max = clip_range
        
    def compute(self, df):
        """执行自适应通道计算
        Args:
            df (DataFrame): 必须包含close价格列
        Returns:
            DataFrame: 新增列[MA_Base, MA_Upper, MA_Lower, Envelope_Pct]
        """
        df = df.copy()
        
        # 计算基础均线
        df['MA_Base'] = df['close'].rolling(self.base_window).mean()
        
        # 计算自适应包络百分比
        df['Envelope_Pct'] = self._calculate_adaptive_pct(df)
        
        # 生成通道带
        df['MA_Upper'] = df['MA_Base'] * (1 + df['Envelope_Pct'])
        df['MA_Lower'] = df['MA_Base'] * (1 - df['Envelope_Pct'])
        
        return df.dropna()

    def _calculate_adaptive_pct(self, df):
        """核心波动率计算逻辑"""
        # 计算日收益率
        returns = df['close'].pct_change()
        
        # 计算波动率(滚动标准差)
        volatility = returns.rolling(self.vol_window).std()
        
        # 波动率缩放与归一化
        envelope_pct = volatility * self.scale_factor
        
        # 百分比范围限制
        return envelope_pct.clip(lower=self.clip_min, upper=self.clip_max)

# ==== 测试代码 ====
if __name__ == "__main__":
    # 生成测试数据（正态分布随机波动）
     # 初始化数据获取器
    fetcher = DataFetcher(
        #symbol='588000',
        symbol='159995',
        #symbol='510050',
        start_date='20200301',
        end_date='20250310'
    )

    # 构建数据文件路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))  # 上两级到项目根目录
    file_name = "159995_20250311_090320.csv"  # 假设数据文件在data目录下
    data_path = os.path.join(project_root, 'data', file_name)  # 假设数据文件在data目录下
    
    # 从CSV文件加载数据
    raw_df = pd.read_csv(data_path, parse_dates=['date'], index_col='date')
    test_df = pd.DataFrame({'close': raw_df['close']}, index=raw_df.index)

    # 初始化自适应通道计算器
    adapter = AdaptiveMAEnvelope(
        base_window=40,
        vol_window=20,
        scale_factor=3.8,  # 根据标的波动特性调整
        clip_range=(0.025, 0.12)  # 科创50ETF适用较宽范围
    )
    
    # 执行计算
    result_df = adapter.compute(test_df)
    
    # 修正路径设置（原错误行）
    current_dir = os.path.dirname(os.path.abspath(__file__))  # 当前文件所在目录
    project_root = os.path.dirname(os.path.dirname(current_dir))  # 上两级目录（src/factor_engine → src → 项目根目录）
    factor_dir = os.path.join(project_root, 'data')  # 正确拼接路径
    os.makedirs(factor_dir, exist_ok=True)  # 自动创建目录
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    save_path = os.path.join(factor_dir, f'factor_Adaptive_MA_Envelope_{timestamp}.csv')
    result_df.to_csv(save_path)
    
    # 结果分析
    print("波动率自适应通道统计摘要：")
    print(result_df[['Envelope_Pct', 'MA_Upper', 'MA_Lower']].describe())
    
    # 可视化片段示例
    plt.figure(figsize=(12,6))
    plt.plot(result_df['close'], label='Price', alpha=0.5)
    plt.plot(result_df['MA_Base'], label='MA Base', linestyle='--')
    plt.fill_between(result_df.index, 
                    result_df['MA_Upper'], 
                    result_df['MA_Lower'], 
                    color='gray', alpha=0.2)
    plt.title(f"{fetcher.symbol} Adaptive MA Envelope (Volatility Scaled)")  # 修正符号引用
    plt.legend()
    plt.show()