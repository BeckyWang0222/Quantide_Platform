# -*- coding: utf-8 -*-
"""
交易时间验证器
用于验证数据是否在交易时间内，确保数据质量
"""
from datetime import datetime, time, date
from typing import List, Optional
import logging


class TradingTimeValidator:
    """交易时间验证器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # A股交易时间配置
        self.trading_sessions = {
            'morning': {
                'start': time(9, 30, 0),    # 09:30:00
                'end': time(11, 30, 0)      # 11:30:00
            },
            'afternoon': {
                'start': time(13, 0, 0),    # 13:00:00
                'end': time(15, 0, 0)       # 15:00:00
            }
        }
        
        # 节假日列表（可以从外部配置文件加载）
        self.holidays = set([
            # 2024年节假日示例
            date(2024, 1, 1),   # 元旦
            date(2024, 2, 10),  # 春节
            date(2024, 2, 11),
            date(2024, 2, 12),
            date(2024, 2, 13),
            date(2024, 2, 14),
            date(2024, 2, 15),
            date(2024, 2, 16),
            date(2024, 2, 17),
            # 可以继续添加其他节假日
        ])
    
    def is_trading_day(self, dt: datetime) -> bool:
        """
        判断是否为交易日
        
        Args:
            dt: 要检查的日期时间
            
        Returns:
            bool: 是否为交易日
        """
        # 检查是否为周末
        if dt.weekday() >= 5:  # 周六=5, 周日=6
            return False
        
        # 检查是否为节假日
        if dt.date() in self.holidays:
            return False
        
        return True
    
    def is_trading_time(self, dt: datetime) -> bool:
        """
        判断是否为交易时间
        
        Args:
            dt: 要检查的日期时间
            
        Returns:
            bool: 是否为交易时间
        """
        # 首先检查是否为交易日
        if not self.is_trading_day(dt):
            return False
        
        current_time = dt.time()
        
        # 检查上午交易时间
        morning_start = self.trading_sessions['morning']['start']
        morning_end = self.trading_sessions['morning']['end']
        
        # 检查下午交易时间
        afternoon_start = self.trading_sessions['afternoon']['start']
        afternoon_end = self.trading_sessions['afternoon']['end']
        
        # 判断是否在交易时间段内
        is_morning_session = morning_start <= current_time <= morning_end
        is_afternoon_session = afternoon_start <= current_time <= afternoon_end
        
        return is_morning_session or is_afternoon_session
    
    def filter_trading_time_data(self, data_list: List[dict]) -> List[dict]:
        """
        过滤出交易时间内的数据
        
        Args:
            data_list: 数据列表，每个元素应包含'frame'或'time'字段
            
        Returns:
            List[dict]: 过滤后的数据列表
        """
        filtered_data = []
        
        for data in data_list:
            # 尝试获取时间字段
            dt = None
            if 'frame' in data:
                if isinstance(data['frame'], str):
                    dt = datetime.fromisoformat(data['frame'])
                elif isinstance(data['frame'], datetime):
                    dt = data['frame']
            elif 'time' in data:
                if isinstance(data['time'], str):
                    dt = datetime.fromisoformat(data['time'])
                elif isinstance(data['time'], datetime):
                    dt = data['time']
            
            if dt and self.is_trading_time(dt):
                filtered_data.append(data)
            elif dt:
                self.logger.debug(f"过滤非交易时间数据: {dt}")
        
        return filtered_data
    
    def validate_bar_data(self, bar_data: dict) -> bool:
        """
        验证分钟线数据是否在交易时间内
        
        Args:
            bar_data: 分钟线数据字典
            
        Returns:
            bool: 是否为有效的交易时间数据
        """
        try:
            frame = bar_data.get('frame')
            if not frame:
                self.logger.warning("分钟线数据缺少frame字段")
                return False
            
            if isinstance(frame, str):
                dt = datetime.fromisoformat(frame)
            elif isinstance(frame, datetime):
                dt = frame
            else:
                self.logger.warning(f"无效的frame字段类型: {type(frame)}")
                return False
            
            is_valid = self.is_trading_time(dt)
            
            if not is_valid:
                self.logger.debug(f"非交易时间数据: {dt}")
            
            return is_valid
            
        except Exception as e:
            self.logger.error(f"验证分钟线数据失败: {e}")
            return False
    
    def validate_tick_data(self, tick_data: dict) -> bool:
        """
        验证分笔数据是否在交易时间内
        
        Args:
            tick_data: 分笔数据字典
            
        Returns:
            bool: 是否为有效的交易时间数据
        """
        try:
            time_field = tick_data.get('time')
            if not time_field:
                self.logger.warning("分笔数据缺少time字段")
                return False
            
            if isinstance(time_field, str):
                dt = datetime.fromisoformat(time_field)
            elif isinstance(time_field, datetime):
                dt = time_field
            else:
                self.logger.warning(f"无效的time字段类型: {type(time_field)}")
                return False
            
            is_valid = self.is_trading_time(dt)
            
            if not is_valid:
                self.logger.debug(f"非交易时间分笔数据: {dt}")
            
            return is_valid
            
        except Exception as e:
            self.logger.error(f"验证分笔数据失败: {e}")
            return False
    
    def get_trading_sessions_for_date(self, target_date: date) -> List[dict]:
        """
        获取指定日期的交易时间段
        
        Args:
            target_date: 目标日期
            
        Returns:
            List[dict]: 交易时间段列表
        """
        if not self.is_trading_day(datetime.combine(target_date, time.min)):
            return []
        
        sessions = []
        
        # 上午交易时间段
        morning_start = datetime.combine(target_date, self.trading_sessions['morning']['start'])
        morning_end = datetime.combine(target_date, self.trading_sessions['morning']['end'])
        sessions.append({
            'session': 'morning',
            'start': morning_start,
            'end': morning_end
        })
        
        # 下午交易时间段
        afternoon_start = datetime.combine(target_date, self.trading_sessions['afternoon']['start'])
        afternoon_end = datetime.combine(target_date, self.trading_sessions['afternoon']['end'])
        sessions.append({
            'session': 'afternoon',
            'start': afternoon_start,
            'end': afternoon_end
        })
        
        return sessions
    
    def add_holiday(self, holiday_date: date):
        """添加节假日"""
        self.holidays.add(holiday_date)
    
    def remove_holiday(self, holiday_date: date):
        """移除节假日"""
        self.holidays.discard(holiday_date)
    
    def get_statistics(self, data_list: List[dict]) -> dict:
        """
        获取数据过滤统计信息
        
        Args:
            data_list: 原始数据列表
            
        Returns:
            dict: 统计信息
        """
        total_count = len(data_list)
        valid_count = 0
        invalid_count = 0
        
        for data in data_list:
            if 'frame' in data:
                is_valid = self.validate_bar_data(data)
            elif 'time' in data:
                is_valid = self.validate_tick_data(data)
            else:
                is_valid = False
            
            if is_valid:
                valid_count += 1
            else:
                invalid_count += 1
        
        return {
            'total_count': total_count,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'valid_rate': valid_count / total_count if total_count > 0 else 0
        }
