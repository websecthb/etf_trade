import pandas as pd

class DataValidator:
    """数据质量验证引擎"""
    
    @staticmethod
    def validate_symbol(symbol):
        """验证标的代码格式"""
        if not isinstance(symbol, str) or len(symbol) != 6:
            raise ValueError("标的代码必须为6位字符")
            
    def validate_integrity(self, df):
        """执行完整数据校验"""
        self._check_empty(df)
        self._check_price_logic(df)
        self._check_adjustment(df)
        return True

    def _check_empty(self, df):
        """检查空数据"""
        if df is None or df.empty:
            raise ValueError("获取数据为空，请检查日期范围")

    def _check_price_logic(self, df):
        """验证价格合理性"""
        if (df['high'] < df['low']).any():
            raise ValueError("价格数据异常：最高价低于最低价")

    def _check_adjustment(self, df):
        """检查复权数据有效性"""
        if df['close'].iloc[-1] < 0.1:
            raise ValueError("复权数据异常，疑似前复权计算错误")