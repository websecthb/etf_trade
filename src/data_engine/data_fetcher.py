import akshare as ak
import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime

class DataFetcher:
    """统一数据获取引擎"""
    
    def __init__(self, symbol, start_date, end_date=None):
        self.symbol = symbol
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date or datetime.today().strftime('%Y%m%d'))
        print(f"证券代码====,{self.symbol}")

    def fetch_etf_data(self):
        """获取ETF行情数据"""
        try:
            raw_df = ak.fund_etf_hist_em(
                symbol=self.symbol,
                period="daily",
                adjust="hfq"
            )
            cleaned_df = self._clean_data(raw_df)
            
            # 新增数据保存逻辑
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            data_dir = os.path.join(project_root, 'data')
            os.makedirs(data_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_path = os.path.join(data_dir, f'{self.symbol}_{timestamp}.csv')
            cleaned_df.to_csv(save_path)
            print(f"数据已保存至: {save_path}")
            
            return cleaned_df            



        except Exception as e:
            raise ConnectionError(f"数据获取失败: {str(e)}")

    def _clean_data(self, df):
        """数据清洗流水线"""
        return (
            df.rename(columns={
                '日期': 'date', '开盘': 'open', '最高': 'high',
                '最低': 'low', '收盘': 'close', '成交量': 'volume'
            })
            .pipe(lambda df: df.assign(
                date=pd.to_datetime(df['date']),
                volume=df['volume'] * 100,  # 转换交易量单位（手→股）
                symbol=self.symbol
            ))
            .set_index('date')
            .loc[self.start_date:self.end_date]
            .sort_index()
            .replace(0, np.nan)
            .dropna()  
        )
