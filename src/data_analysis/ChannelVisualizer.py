import pandas as pd
from matplotlib.figure import Figure
from matplotlib.axes import Axes
from matplotlib.gridspec import GridSpec
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from typing import Tuple, Optional, Union

class ChannelVisualizer:
    def __init__(self, 
                 file_path: Union[str, Path],
                 symbol: str = "159995",
                 window: int = 20,
                 band_pct: float = 0.02,
                 figure_size: tuple = (16, 10)):
        self.file_path = Path(file_path)
        self.symbol = symbol
        self.window = window
        self.band_pct = band_pct
        self.figure_size = figure_size
        self.df = self._load_and_validate()
           
    # 修复中文显示问题
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False

    def _load_and_validate(self) -> pd.DataFrame:
        """加载并校验数据文件"""
        if not self.file_path.exists():
            raise FileNotFoundError(f"[Critical] 数据文件未找到：{self.file_path}")

        df = pd.read_csv(
            self.file_path,
            parse_dates=['date'],
            index_col='date'
        ).sort_index()

        # 修正后的列名校验
        required_cols = ['close', 'MA_Base', 'MA_Upper', 
                        'MA_Lower', 'Signal']
        missing = [col for col in required_cols if col not in df.columns]
        
        if missing:
            raise ValueError(
                f"数据校验失败！缺失字段：{missing}\n"
                f"当前可用字段：{list(df.columns)}\n"
                f"请检查输入文件：{self.file_path}"
            )
        
        # 动态计算通道宽度百分比
        df['Band_Pct'] = (df['MA_Upper'] - df['MA_Lower']) / (2 * df['MA_Base'])
        return df

    def _create_figure(self) -> Tuple[Figure, GridSpec]:
        """创建专业级图表布局"""
        fig = plt.figure(figsize=self.figure_size)
        gs = fig.add_gridspec(3, 1, height_ratios=[3, 1, 1], hspace=0.05)
        return fig, gs

    def _plot_price_band(self, ax: Axes) -> None:
        """绘制价格通道主图"""
        avg_pct = self.df['Band_Pct'].mean() * 100
        
        ax.plot(self.df['close'], label='收盘价', color='#4169E1', alpha=0.9)
        ax.plot(self.df['MA_Base'], 
               label=f'{self.window}周期基准线', 
               color='#FF8C00', 
               linestyle='--')
        ax.plot(self.df['MA_Upper'], 
               label=f'上轨', 
               color='#32CD32', 
               linewidth=1.2)
        ax.plot(self.df['MA_Lower'], 
               label=f'下轨', 
               color='#FF4500', 
               linewidth=1.2)
        ax.fill_between(self.df.index,
                       self.df['MA_Upper'],
                       self.df['MA_Lower'],
                       color='grey', 
                       alpha=0.1)
        ax.set_title(f'{self.symbol} 自适应通道信号分析 | 平均宽度：{avg_pct:.2f}%', 
                    fontsize=14, pad=12)
        ax.legend(loc='upper left', framealpha=0.9)

    def _plot_band_width(self, ax: Axes) -> None:
        """绘制通道宽度子图"""
        ax.plot(self.df['Band_Pct'], 
               label='通道宽度', 
               color='#6A5ACD')
        mean_band_pct = float(self.df['Band_Pct'].mean())
        ax.axhline(y=mean_band_pct, 
                  color='r', 
                  linestyle='--', 
                  linewidth=0.8)
        ax.set_ylabel('宽度比率 (%)', fontsize=9)

    def _plot_trading_signals(self, ax: Axes) -> None:
        """绘制交易信号子图"""
        ax.plot(self.df['Signal'], 
               label='交易信号', 
               color='#2F4F4F', 
               drawstyle='steps')
        ax.set_yticks([-1, 0, 1])
        ax.set_yticklabels(['做空', '空仓', '做多'], fontsize=8)
        ax.set_ylim(-1.5, 1.5)

    # ==== 修改保存路径类型 ====
    def visualize(self, save_path: Union[str, Path, None] = None) -> None:
        """执行完整可视化流程"""
        fig, gs = self._create_figure()
        ax1 = fig.add_subplot(gs[0])
        ax2 = fig.add_subplot(gs[1], sharex=ax1)
        ax3 = fig.add_subplot(gs[2], sharex=ax1)

        self._plot_price_band(ax1)
        self._plot_band_width(ax2)
        self._plot_trading_signals(ax3)

        if save_path:
            save_path = Path(save_path)  # 统一转换为Path对象
            save_path.parent.mkdir(parents=True, exist_ok=True)
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"图表已保存至：{save_path}")
        plt.show()

if __name__ == "__main__":
    # 现在可以接受两种路径格式
    visualizer = ChannelVisualizer(
        file_path="E://gzhtemp//etf_trade_v1//data//signal_Adaptive_MA_Envelope_20250311_133351.csv",  # 字符串路径
        # 或 Path 对象路径
        # file_path=Path("data") / "signal_...csv",
        symbol="159995",
        window=20,
        band_pct=0.02
    )
    visualizer.visualize(
        save_path=Path("output") / "channel_analysis.png"  # 保持Path对象传递
    )