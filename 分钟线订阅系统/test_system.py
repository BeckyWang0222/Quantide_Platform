# -*- coding: utf-8 -*-
"""
系统测试脚本
用于测试各个组件的功能
"""
import sys
import time
from datetime import datetime, timedelta
from database import RedisManager, ClickHouseManager
from data_processor import BarDataSynthesizer, DataMerger
from models import TickData, BarData, HistoricalDataRequest
from qmt_historical_fetcher import QMTHistoricalFetcher
from trading_time_validator import TradingTimeValidator


def test_redis_connection():
    """测试Redis连接"""
    print("测试Redis连接...")
    try:
        redis_manager = RedisManager()
        info = redis_manager.get_system_info()
        print(f"✓ Redis连接成功: {info}")
        return True
    except Exception as e:
        print(f"✗ Redis连接失败: {e}")
        return False


def test_clickhouse_connection():
    """测试ClickHouse连接"""
    print("测试ClickHouse连接...")
    try:
        clickhouse_manager = ClickHouseManager()
        info = clickhouse_manager.get_system_info()
        print(f"✓ ClickHouse连接成功: {info}")
        return True
    except Exception as e:
        print(f"✗ ClickHouse连接失败: {e}")
        return False


def test_data_synthesis():
    """测试数据合成功能"""
    print("测试数据合成功能...")
    try:
        synthesizer = BarDataSynthesizer()

        # 模拟分笔数据
        base_time = datetime.now().replace(second=0, microsecond=0)
        for i in range(5):
            tick_data = TickData(
                symbol="TEST001",
                time=base_time + timedelta(seconds=i*10),
                price=10.0 + i * 0.1,
                volume=1000,
                amount=10000 + i * 100
            )
            synthesizer.add_tick_data(tick_data)

        # 检查合成结果
        bars_1min = synthesizer.get_latest_bars("TEST001", 1)
        if bars_1min:
            print(f"✓ 数据合成成功，生成了 {len(bars_1min)} 条1分钟线")
            return True
        else:
            print("✗ 数据合成失败，未生成分钟线数据")
            return False

    except Exception as e:
        print(f"✗ 数据合成测试失败: {e}")
        return False


def test_redis_operations():
    """测试Redis操作"""
    print("测试Redis操作...")
    try:
        redis_manager = RedisManager()

        # 测试分笔数据发布
        tick_data = TickData(
            symbol="TEST001",
            time=datetime.now(),
            price=10.50,
            volume=1000,
            amount=10500
        )
        redis_manager.publish_tick_data(tick_data)

        # 测试分钟线数据发布
        bar_data = BarData(
            symbol="TEST001",
            frame=datetime.now().replace(second=0, microsecond=0),
            open=10.0,
            high=10.5,
            low=9.9,
            close=10.3,
            vol=5000,
            amount=51000
        )
        redis_manager.publish_bar_data(bar_data, 1)

        # 测试数据消费
        consumed_tick = redis_manager.consume_tick_data(timeout=1)
        consumed_bar = redis_manager.consume_bar_data(1, timeout=1)

        if consumed_tick and consumed_bar:
            print("✓ Redis操作测试成功")
            return True
        else:
            print("✗ Redis操作测试失败，数据消费异常")
            return False

    except Exception as e:
        print(f"✗ Redis操作测试失败: {e}")
        return False


def test_clickhouse_operations():
    """测试ClickHouse操作"""
    print("测试ClickHouse操作...")
    try:
        clickhouse_manager = ClickHouseManager()

        # 测试数据插入
        test_bars = []
        base_time = datetime.now().replace(second=0, microsecond=0)
        for i in range(3):
            bar_data = BarData(
                symbol="TEST001",
                frame=base_time + timedelta(minutes=i),
                open=10.0 + i * 0.1,
                high=10.2 + i * 0.1,
                low=9.8 + i * 0.1,
                close=10.1 + i * 0.1,
                vol=1000 + i * 100,
                amount=10000 + i * 1000
            )
            test_bars.append(bar_data)

        clickhouse_manager.insert_bar_data(test_bars, 1)

        # 测试数据查询
        start_time = base_time - timedelta(minutes=1)
        end_time = base_time + timedelta(minutes=5)
        queried_bars = clickhouse_manager.query_bar_data(
            "TEST001", start_time, end_time, 1
        )

        if len(queried_bars) >= len(test_bars):
            print(f"✓ ClickHouse操作测试成功，插入 {len(test_bars)} 条，查询到 {len(queried_bars)} 条")
            return True
        else:
            print(f"✗ ClickHouse操作测试失败，插入 {len(test_bars)} 条，查询到 {len(queried_bars)} 条")
            return False

    except Exception as e:
        print(f"✗ ClickHouse操作测试失败: {e}")
        return False


def test_data_merger():
    """测试数据合并功能"""
    print("测试数据合并功能...")
    try:
        # 创建测试数据
        base_time = datetime.now().replace(second=0, microsecond=0)

        redis_data = [
            BarData(
                symbol="TEST001",
                frame=base_time,
                open=10.0, high=10.2, low=9.8, close=10.1,
                vol=1000, amount=10100
            )
        ]

        clickhouse_data = [
            BarData(
                symbol="TEST001",
                frame=base_time - timedelta(minutes=1),
                open=9.9, high=10.1, low=9.7, close=10.0,
                vol=1200, amount=12000
            )
        ]

        # 测试合并
        merged_data = DataMerger.merge_bar_data(redis_data, clickhouse_data)

        if len(merged_data) == 2:
            print("✓ 数据合并测试成功")
            return True
        else:
            print(f"✗ 数据合并测试失败，期望2条记录，实际{len(merged_data)}条")
            return False

    except Exception as e:
        print(f"✗ 数据合并测试失败: {e}")
        return False


def test_historical_data_fetcher():
    """测试历史数据获取功能"""
    print("测试历史数据获取功能...")
    try:
        fetcher = QMTHistoricalFetcher()

        # 创建测试请求
        start_time = datetime.now() - timedelta(hours=2)
        end_time = datetime.now() - timedelta(hours=1)

        request = HistoricalDataRequest(
            start_time=start_time,
            end_time=end_time,
            symbols=["TEST001"],
            periods=[1, 5]
        )

        # 启动获取任务
        response = fetcher.fetch_historical_data(request)

        if response.success:
            print(f"✓ 历史数据获取任务启动成功，任务ID: {response.task_id}")

            # 等待一段时间让任务执行
            time.sleep(3)

            # 检查任务状态
            status = fetcher.get_task_status(response.task_id)
            if status:
                print(f"✓ 任务状态查询成功: {status['status']}")
                print(f"  处理进度: {status['processed_symbols']}/{status['total_symbols']}")
                print(f"  有效记录数: {status['total_records']}")
                print(f"  过滤记录数: {status.get('filtered_records', 0)}")
                return True
            else:
                print("✗ 任务状态查询失败")
                return False
        else:
            print(f"✗ 历史数据获取任务启动失败: {response.message}")
            return False

    except Exception as e:
        print(f"✗ 历史数据获取测试失败: {e}")
        return False


def test_trading_time_validator():
    """测试交易时间验证器"""
    print("测试交易时间验证器...")
    try:
        validator = TradingTimeValidator()

        # 测试交易时间判断
        # 工作日上午10点（交易时间）
        trading_time = datetime(2024, 1, 15, 10, 0, 0)  # 周一
        if validator.is_trading_time(trading_time):
            print("✓ 交易时间判断正确")
        else:
            print("✗ 交易时间判断错误")
            return False

        # 周末（非交易时间）
        weekend_time = datetime(2024, 1, 13, 10, 0, 0)  # 周六
        if not validator.is_trading_time(weekend_time):
            print("✓ 周末时间判断正确")
        else:
            print("✗ 周末时间判断错误")
            return False

        # 测试数据验证
        valid_bar_data = {
            'frame': trading_time,
            'symbol': 'TEST001',
            'open': 10.0,
            'high': 10.5,
            'low': 9.5,
            'close': 10.2,
            'vol': 1000,
            'amount': 10200
        }

        if validator.validate_bar_data(valid_bar_data):
            print("✓ 有效分钟线数据验证正确")
        else:
            print("✗ 有效分钟线数据验证错误")
            return False

        # 测试无效数据
        invalid_bar_data = valid_bar_data.copy()
        invalid_bar_data['frame'] = weekend_time

        if not validator.validate_bar_data(invalid_bar_data):
            print("✓ 无效分钟线数据验证正确")
        else:
            print("✗ 无效分钟线数据验证错误")
            return False

        print("✓ 交易时间验证器测试成功")
        return True

    except Exception as e:
        print(f"✗ 交易时间验证器测试失败: {e}")
        return False





def test_data_storage_separation():
    """测试数据存储分离"""
    print("测试数据存储分离...")
    try:
        redis_manager = RedisManager()

        # 创建测试数据
        current_time = datetime.now().replace(second=0, microsecond=0)

        # 测试当日数据存储
        current_bar = BarData(
            symbol="TEST001",
            frame=current_time,
            open=10.0, high=10.2, low=9.8, close=10.1,
            vol=1000, amount=10100
        )

        # 发布当日数据（is_historical=False）
        redis_manager.publish_bar_data(current_bar, 1, is_historical=False)

        # 检查当日数据存储
        current_data = redis_manager.get_current_bar_data(1, "TEST001")
        if current_data and len(current_data) > 0:
            print("✓ 当日数据存储到Redis成功")
        else:
            print("✗ 当日数据存储失败")
            return False

        # 测试历史数据发布
        historical_time = current_time - timedelta(days=1)
        historical_bar = BarData(
            symbol="TEST001",
            frame=historical_time,
            open=9.5, high=9.8, low=9.3, close=9.7,
            vol=1200, amount=11640
        )

        # 发布历史数据（is_historical=True）
        redis_manager.publish_bar_data(historical_bar, 1, is_historical=True)

        # 检查历史数据队列
        queue_length = redis_manager.client.llen("bar_data_1min")
        if queue_length > 0:
            print("✓ 历史数据发布到队列成功")
        else:
            print("✗ 历史数据发布失败")
            return False

        print("✓ 数据存储分离测试成功")
        return True

    except Exception as e:
        print(f"✗ 数据存储分离测试失败: {e}")
        return False


def run_all_tests():
    """运行所有测试"""
    print("=" * 50)
    print("开始系统测试")
    print("=" * 50)

    tests = [
        ("Redis连接", test_redis_connection),
        ("ClickHouse连接", test_clickhouse_connection),
        ("数据合成", test_data_synthesis),
        ("Redis操作", test_redis_operations),
        ("ClickHouse操作", test_clickhouse_operations),
        ("数据合并", test_data_merger),
        ("历史数据获取", test_historical_data_fetcher),
        ("交易时间验证", test_trading_time_validator),
        ("数据存储分离", test_data_storage_separation),
    ]

    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}测试:")
        result = test_func()
        results.append((test_name, result))
        time.sleep(1)

    print("\n" + "=" * 50)
    print("测试结果汇总:")
    print("=" * 50)

    passed = 0
    for test_name, result in results:
        status = "✓ 通过" if result else "✗ 失败"
        print(f"{test_name}: {status}")
        if result:
            passed += 1

    print(f"\n总计: {passed}/{len(tests)} 个测试通过")

    if passed == len(tests):
        print("🎉 所有测试通过！系统可以正常运行。")
    else:
        print("⚠️  部分测试失败，请检查配置和环境。")


if __name__ == "__main__":
    run_all_tests()
