import backtrader as bt

class SignalDataFeeder(bt.feeds.PandasData):
    """
    增强版数据加载器，修复日期和列名问题
    确保输入的DataFrame包含以下列：
    - date (datetime类型索引)
    - close (收盘价)
    - MA_Upper (上轨)
    - MA_Lower (下轨)
    - Signal (交易信号)
    """
    
    # 定义扩展数据线
    lines = ('ma_upper', 'ma_lower', 'signal')
    
    # 列名映射（严格匹配CSV列名）
    params = (
        ('ma_upper', 'MA_Upper'),  # 注意大小写匹配
        ('ma_lower', 'MA_Lower'),
        ('signal', 'Signal'),
        ('datetime', None),       # 使用索引作为时间序列
        ('close', 'close'),
        ('open', -1),             # 不需要的列设为-1
        ('high', -1),
        ('low', -1),
        ('volume', -1),
        ('openinterest', -1)
    )