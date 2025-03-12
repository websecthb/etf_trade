import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Any

class StrategyAnalyzer:
    """专业级策略分析器（最终修复版v3）"""

    def __init__(self, 
                 signal_path: str, 
                 output_dir: str = "analysis_reports",
                 price_col: str = "close",
                 signal_col: str = "Signal",
                 upper_col: str = "MA_Upper",
                 lower_col: str = "MA_Lower"):
        self.signal_path: Path = Path(signal_path)
        self.output_dir: Path = Path(output_dir)
        self.price_col: str = price_col
        self.signal_col: str = signal_col
        self.upper_col: str = upper_col
        self.lower_col: str = lower_col
        
        self.df: Optional[pd.DataFrame] = None
        self.metrics: Dict[str, Any] = {
            'total_days': 0,
            'avg_holding_days': 0.0,
            'upper_breakouts': 0,
            'lower_breakouts': 0,
            'width_stats': {'mean':0.0, 'median':0.0, 'std':0.0}
        }

        self._validate_signal_path()

    def _validate_signal_path(self) -> None:
        if not self.signal_path.exists():
            raise FileNotFoundError(f"信号文件不存在: {self.signal_path}")

    def load_data(self) -> "StrategyAnalyzer":
        try:
            df = pd.read_csv(
                self.signal_path
                # parse_dates=['date'],
                # index_col='date',
                # infer_datetime_format=True,
                # dayfirst=True
            )
            
            if not isinstance(df, pd.DataFrame):
                raise TypeError("数据必须为DataFrame格式")
                
            self.df = self._post_process_dataframe(df)
            self._validate_required_columns()
            self._generate_derived_columns()
            return self
        except Exception as e:
            self.df = None
            raise RuntimeError(f"数据加载失败: {str(e)}")

    def _post_process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.sort_index()
            .assign(date=lambda x: pd.to_datetime(x.date, errors='coerce'))
            .dropna(subset=['date'])
            .set_index('date')
        )

    def _validate_required_columns(self) -> None:
        if self.df is None:
            raise ValueError("数据未加载，无法验证列")
        required_cols = {self.price_col, self.signal_col, self.upper_col, self.lower_col}
        missing_cols = required_cols - set(self.df.columns)
        if missing_cols:
            raise ValueError(f"缺失必要字段: {', '.join(missing_cols)}")

    def _generate_derived_columns(self) -> None:
        if self.df is None:
            return
            
        base = self.df[self.upper_col].replace(0, 1e-6)
        self.df['Band_Width'] = (self.df[self.upper_col] - self.df[self.lower_col]) / base
        
        self.df['Upper_Breakout'] = (
            self.df[self.price_col].gt(self.df[self.upper_col])
            .astype(int)
            .fillna(0)
        )
        self.df['Lower_Breakout'] = (
            self.df[self.price_col].lt(self.df[self.lower_col])
            .astype(int)
            .fillna(0)
        )

    def calculate_metrics(self) -> "StrategyAnalyzer":
        if self.df is None:
            return self  # 明确返回 self
            
        signal_series = self.df[self.signal_col].astype(int).fillna(0)
        signal_counts = signal_series.value_counts()
        active_signals = (signal_counts.get(1, 0) or 0) + (signal_counts.get(-1, 0) or 0)
        
        self.metrics['active_signals'] = active_signals
        self.metrics['signal_ratio'] = active_signals / len(self.df) if len(self.df) > 0 else 0.0

        self.metrics['upper_breakouts'] = self.df['Upper_Breakout'].sum() if 'Upper_Breakout' in self.df else 0
        self.metrics['lower_breakouts'] = self.df['Lower_Breakout'].sum() if 'Lower_Breakout' in self.df else 0

        width_series = self.df['Band_Width'].dropna()
        self.metrics['width_stats'] = {
            'mean': width_series.mean() if not width_series.empty else 0.0,
            'median': width_series.median() if not width_series.empty else 0.0,
            'std': width_series.std() if not width_series.empty else 0.0
        }

        signal_changes = int(signal_series.diff().abs().gt(0).sum())
        self.metrics['total_trades'] = signal_changes // 2
        self.metrics['avg_holding_days'] = (
            len(signal_series) / self.metrics['total_trades']
            if self.metrics['total_trades'] > 0 else 0.0
        )
        return self  # 支持链式调用

    def generate_report(self, filename: Optional[str] = None) -> str:
        if self.df is None or not self.metrics:
            return "数据加载失败，无法生成报告"
            
        start_date = "N/A"
        end_date = "N/A"
        if isinstance(self.df.index, pd.DatetimeIndex):
            start_date = self.df.index[0].strftime('%Y-%m-%d')
            end_date = self.df.index[-1].strftime('%Y-%m-%d')
        
        report = f"""
        ======= 量化策略分析报告 =======
        时间范围: {start_date} - {end_date}
        ----------------------------------
        1. 基础统计
        - 总交易日数: {len(self.df)}
        - 有效信号占比: {self.metrics['signal_ratio']:.1%}
        - 平均持仓周期: {self.metrics['avg_holding_days']:.1f}天
        
        2. 突破统计
        - 上轨突破次数: {self.metrics['upper_breakouts']} (占比{self.metrics['upper_breakouts']/len(self.df):.1%})
        - 下轨突破次数: {self.metrics['lower_breakouts']} (占比{self.metrics['lower_breakouts']/len(self.df):.1%})
        
        3. 通道特征
        - 宽度均值: {self.metrics['width_stats']['mean']:.3f}
        - 宽度中位数: {self.metrics['width_stats']['median']:.3f}
        - 宽度波动率: {self.metrics['width_stats']['std']:.3f}
        """
        
        self._save_report(report, filename)
        return report

    def _save_report(self, content: str, filename: Optional[str]) -> None:
        try:
            self.output_dir.mkdir(exist_ok=True)
            final_filename = filename or f"report_{pd.Timestamp.now().strftime('%Y%m%d')}.txt"
            file_path = self.output_dir / final_filename
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"报告已成功保存至: {file_path}")
        except Exception as e:
            print(f"文件保存失败: {str(e)}")

# 使用示例
if __name__ == "__main__":
    try:
        analyzer = (
            StrategyAnalyzer(
                signal_path="E://gzhtemp//etf_trade_v1//data//signal_Adaptive_MA_Envelope_20250311_133351.csv",
                upper_col="MA_Upper",
                lower_col="MA_Lower"
            )
            .load_data()
            .calculate_metrics()
        )
        
        print(analyzer.generate_report())
    except Exception as e:
        print(f"执行失败: {str(e)}")