# backtest.py
import pandas as pd
import numpy as np
import backtrader as bt
import sys
import os


# 修正路径：向上两级到项目根目录（假设backtest.py在src/backtrader_engine/）
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(project_root)

# 现在应该可以正确导入
from src.strategy.MaStrategy import  AdaptiveMAEnvelopeStrategy
from src.strategy.SignalDataFeeder import SignalDataFeeder

def run_backtest():
    cerebro = bt.Cerebro()
    
    # 加载原始数据
    data_path = 'E:/gzhtemp/etf_trade_v1/data/signal_Adaptive_MA_Envelope_20250311_133351.csv'
    df = pd.read_csv(data_path)
    
    # 关键修复1：日期格式转换（假设原始日期是YYYYMMDD格式的整数）
    df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y-%m-%d')
    df.set_index('date', inplace=True)
    
    # 关键修复2：数据标准化处理
    numeric_cols = ['close', 'MA_Upper', 'MA_Lower']
    df[numeric_cols] = df[numeric_cols].apply(lambda x: x.abs().ffill())
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    # df.fillna(method='bfill', inplace=True)
    df = df.bfill().ffill()
    
    # 数据验证
    print("\n=== 数据摘要 ===")
    print(f"时间范围: {df.index.min()} 至 {df.index.max()}")
    print(f"数据列:\n{df[numeric_cols].describe()}")
    
    # 创建数据源（严格匹配列名）
    data = SignalDataFeeder(
        dataname=df,
        ma_upper='MA_Upper',  # 必须与DataFrame列名完全一致
        ma_lower='MA_Lower',
        signal='Signal',
        close='close'
    )
    cerebro.adddata(data)
    
    # 策略配置
    cerebro.addstrategy(
        AdaptiveMAEnvelopeStrategy,
        risk_per_trade=0.002,
        max_price_change=0.05,
        min_position=100,
        slippage=0.01,
        printlog=True
    )
    
    # 资金配置（调整为合理规模）
    initial_cash = 10_000_000.0  # 1000万更易验证
    # cerebro.broker.set_slippage_perc(perc=0.001) #0.1%滑点
    cerebro.broker.setcash(initial_cash)
    cerebro.broker.setcommission(
        commission=0.00015,
        margin=None,  # 固定滑点
        mult=0.001    # 百分比滑点
    )
    
    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.0)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    
    # 执行回测
    print(f"\n初始资金: {initial_cash:,.2f}")
    results = cerebro.run()
    
    # 结果分析
    strat = results[0]
    final_value = cerebro.broker.getvalue()
    print(f"\n最终资金: {final_value:,.2f}")
    
    # 性能指标
    if not np.isnan(final_value):
        print(f"夏普比率: {strat.analyzers.sharpe.get_analysis()['sharperatio']:.2f}")
        dd_analysis = strat.analyzers.drawdown.get_analysis()
        print(f"最大回撤: {dd_analysis['max']['drawdown']:.2f}%")
        
        # 交易统计
        trade_analysis = strat.analyzers.trades.get_analysis()
        print(f"\n=== 交易统计 ===")
        print(f"总交易次数: {trade_analysis['total']['total']}")
        print(f"盈利交易比例: {trade_analysis['won']['pnl']['average']:.2%}")
    else:
        print("警告：最终资金计算异常，请检查数据质量")

if __name__ == '__main__':
    run_backtest()