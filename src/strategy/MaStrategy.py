import numpy as np
import backtrader as bt

class AdaptiveMAEnvelopeStrategy(bt.Strategy):
    """
    增强版自适应均线通道策略
    主要修复点：
    1. 数据有效性验证
    2. 头寸计算异常处理
    3. 交易信号过滤
    """
    
    params = (
        ('printlog', True),          # 启用操作日志
        ('risk_per_trade', 0.000002),   # 单笔风险比例
        ('max_price_change', 0.05),  # 最大允许波动率
        ('min_position', 100),      # 最小交易单位
        ('slippage', 0.001),        # 滑点控制
    )

    def __init__(self):
    
        # 数据引用
        self.price = self.data.close
        self.ma_upper = self.data.ma_upper
        self.ma_lower = self.data.ma_lower
        
        # 交易信号
        self.buy_signal = bt.indicators.CrossOver(self.price, self.ma_upper)
        self.sell_signal = bt.indicators.CrossOver(self.price, self.ma_lower)
        
        # 数据验证条件（修正关键错误）
        self.data_valid = bt.And(
            self.price > 0,      # 直接使用布尔表达式
            self.ma_upper > 0,
            self.ma_lower > 0
        )
        
        # 交易记录
        self.trade_count = 0
        self.last_price = None 

    def next(self):
        """ 核心交易逻辑 """
        # 数据有效性检查
        if not self._validate_data():
            return
            
        # 波动性过滤
        if self._price_change_exceeded():
            return
            
        # 头寸计算
        size = self._calculate_position()
        if size <= 0:
            return
            
        # 执行交易
        self._execute_trade(size)

    def _validate_data(self):
        """ 三层数据验证 """
        if not self.data_valid[0]:
            self.log("无效数据，跳过周期")
            return False
        if any(map(np.isnan, [self.price[0], self.ma_upper[0], self.ma_lower[0]])):
            self.log("检测到NaN值")
            return False
        if self.price[0] <= 0:
            self.log(f"异常价格: {self.price[0]}")
            return False
        return True

    def _price_change_exceeded(self):
        """ 价格波动率检查 """
        if self.last_price is None:
            self.last_price = self.price[0]
            return False
            
        change = abs(self.price[0] - self.last_price) / self.last_price
        self.last_price = self.price[0]
        if change > self.p.max_price_change:
            self.log(f"波动过大: {change:.2%}")
            return True
        return False

    def _calculate_position(self):
        """ 安全头寸计算 """
        try:
            capital = self.broker.getvalue()
            if capital <= 0:
                raise ValueError("无效资金")
                
            risk_amount = capital * self.p.risk_per_trade
            size = int(risk_amount / self.price[0])
            return max(size, self.p.min_position)
            
        except Exception as e:
            self.log(f"头寸计算失败: {str(e)}")
            return 0

# def _execute_trade(self, size):
#     """ 执行交易订单 """
#     if not self.position:
#         if self.buy_signal[0] == 1:
#             # 改为市价单，滑点由Broker统一处理
#             self.buy(size=size, exectype=bt.Order.Market)
#             self.trade_count += 1
#             self.log(f'买入 {size} 股 @ 市价')
#     else:
#         if self.sell_signal[0] == 1:
#             self.close()  # 市价平仓
#             self.log(f'卖出 @ 市价')

    def _execute_trade(self, size):
        """ 执行交易订单 """
        if not self.position:
            if self.buy_signal[0] == 1:
                self.buy(size=size, exectype=bt.Order.Limit, 
                       price=self.price[0]*(1+self.p.slippage))
                self.trade_count += 1
                self.log(f'买入 {size} 股 @ 限价{self.price[0]:.2f}')
        else:
            if self.sell_signal[0] == 1:
                self.close(price=self.price[0]*(1-self.p.slippage))
                self.log(f'卖出 @ 限价{self.price[0]:.2f}')

    def log(self, txt, dt=None, doprint=False):
        """ 增强型日志记录 """
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print(f'[{dt.isoformat()}] {txt}')

    def notify_order(self, order):
        """ 订单状态跟踪 """
        if order.status in [order.Submitted, order.Accepted]:
            self.log(f"废单 {order.getstatusname()}", doprint=True)
            return


        if order.status == order.Completed:
            self.log(f"订单成交 {order.getstatusname()}", doprint=True)
            self.log(f"成交详情 数量: {order.executed.size} 价格: {order.executed.price:.2f}", doprint=True)
            direction = '买入' if order.isbuy() else '卖出'
            exec_info = (
                f'{direction}成交: {order.executed.size}股 @ '
                f'{order.executed.price:.2f}, 成本: {order.executed.value:.2f}, '
                f'手续费: {order.executed.comm:.2f}'
            )
            self.log(exec_info)
            
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f'订单异常: {order.getstatusname()}')

    def stop(self):
        """ 回测结束分析 """
        self.log(f'总交易次数: {self.trade_count}')
        self.log(f'期末资产: {self.broker.getvalue():.2f}')