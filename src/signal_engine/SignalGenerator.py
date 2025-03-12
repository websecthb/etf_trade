import pandas as pd
import numpy as np
import os
from pathlib import Path
from typing import Optional
from datetime import datetime

class SignalGenerator:
    """自适应移动平均线包络策略信号生成器
    
    Attributes:
        upper_band_col (str): 上轨列名，默认'MA_UpperBand'
        lower_band_col (str): 下轨列名，默认'MA_LowerBand'
    """
    
    def __init__(self, input_path: str, output_dir: str = "data",
                 upper_band_col: str = 'MA_UpperBand',
                 lower_band_col: str = 'MA_LowerBand'):
        """
        Args:
            input_path (str): 输入数据文件路径
            output_dir (str): 信号文件输出目录，默认data/
            upper_band_col (str): 上轨列名，默认'MA_UpperBand'
            lower_band_col (str): 下轨列名，默认'MA_LowerBand'
        """
        self.input_path = input_path
        self.output_dir = output_dir
        self.upper_band_col = upper_band_col
        self.lower_band_col = lower_band_col
        self.df: Optional[pd.DataFrame] = None  # 明确类型提示

    def load_data(self) -> 'SignalGenerator':
        """加载原始数据文件（增强类型安全）"""
        try:
            self.df = pd.read_csv(
                self.input_path, 
                parse_dates=['date'], 
                index_col='date'
            )
            print(f"成功加载数据：{self.input_path}")
            return self
        except Exception as e:
            print(f"数据加载失败: {str(e)}")
            self.df = None
            return self
    
    def _validate_columns(self) -> None:
        """类型安全的字段校验"""
        if self.df is None:
            raise ValueError("数据未加载，请先调用 load_data 方法。")
            
        required_columns = ['close', self.upper_band_col, self.lower_band_col]
        missing = [col for col in required_columns if col not in self.df.columns]

        if missing:
            raise ValueError(f"缺失必要字段: {missing}。当前数据字段: {list(self.df.columns)}")

    def _generate_signals(self) -> pd.DataFrame:
        """类型安全的信号生成"""
        self._validate_columns()
        
        # 类型断言确保df不为None
        assert self.df is not None, "数据框不应为None"  
        
        df = self.df.copy()
        upper_cond = df["close"] > df[self.upper_band_col]
        lower_cond = df["close"] < df[self.lower_band_col]
        
        df["Signal"] = np.select(
            condlist=[upper_cond, lower_cond],
            choicelist=[1, -1],
            default=0
        )
        return df
    
    def process(self) -> 'SignalGenerator':
        """类型安全的处理流程"""
        if self.df is None:
            self.load_data()
            
        if self.df is None:
            raise ValueError("数据加载失败，请检查输入文件路径及格式")
        
        self.df = self._generate_signals()
        return self
    
    def save_to_csv(self, filename: Optional[str] = None) -> None:
        """类型安全的文件保存"""
        if self.df is None:
            raise ValueError("没有可供保存的数据，请先执行process()")
        # 修正路径设置（原错误行）
        current_dir = os.path.dirname(os.path.abspath(__file__))  # 当前文件所在目录
        project_root = os.path.dirname(os.path.dirname(current_dir))  # 上两级目录（src/factor_engine → src → 项目根目录）
        signal_file_dir = os.path.join(project_root, 'data')  # 正确拼接路径
    
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        signal_save_path = os.path.join(signal_file_dir, f'signal_Adaptive_MA_Envelope_{timestamp}.csv')
        self.df.to_csv(signal_save_path)
        print(f"[Success] 信号文件已保存至：{signal_save_path}")

# 测试用例
if __name__ == "__main__":
    # 正确路径测试
    valid_processor = SignalGenerator(
        input_path=r"E:\gzhtemp\etf_trade_v1\data\factor_Adaptive_MA_Envelope_20250311_130754.csv",
        upper_band_col="MA_Upper",
        lower_band_col="MA_Lower"
    )
    try:
        valid_processor.load_data().process().save_to_csv()
    except Exception as e:
        print(f"正常用例测试失败: {str(e)}")
    else:
        print("正常用例测试通过")

    