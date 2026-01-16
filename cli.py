"""
CLI 工具 - 交互式连板分析命令行工具

提供以下功能：
1. 查询指定日期的连板股票
2. 按股票代码/名称搜索
3. 连板统计和趋势分析
4. 交互式模式
5. 数据导出
"""
import argparse
import pandas as pd
from pathlib import Path
from typing import Optional, List
import sys
import json
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import track
from rich import print as rprint

from depend.config import config

# 初始化 Rich 控制台
console = Console()


# ==================== 数据存储抽象层 ====================
class DataLoader:
    """数据加载器 - 统一的数据访问接口"""

    def __init__(self):
        self.ladder_data = None
        self.kline_data = None

    def load_ladder_data(self):
        """加载连板阶梯数据"""
        if self.ladder_data is None:
            ladder_file = Path(config.DEFAULT_LADDER_FILE)
            if not ladder_file.exists():
                console.print(f"[red]数据文件不存在: {ladder_file}[/red]")
                console.print("[yellow]请先运行: python main.py full[/yellow]")
                return None

            df = pd.read_parquet(ladder_file)
            self.ladder_data = df
        return self.ladder_data

    def load_kline_data(self):
        """加载 K 线数据"""
        if self.kline_data is None:
            kline_file = Path('data/kline_data.js')
            if kline_file.exists():
                import re
                with open(kline_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                import json
                json_start = content.find('{')
                json_end = content.rfind('}') + 1
                self.kline_data = json.loads(content[json_start:json_end])
            else:
                console.print("[yellow]K线数据文件不存在[/yellow]")
        return self.kline_data


# 创建全局数据加载器
data_loader = DataLoader()


# ==================== 查询命令 ====================
def query_limit_up(date: str, min_days: int = 1, max_days: Optional[int] = None):
    """查询指定日期的涨停股票"""
    df = data_loader.load_ladder_data()
    if df is None:
        return

    # 筛选涨停股票
    df = df[df['is_limit_up'] == True]

    # 将日期转换为合适的格式
    try:
        # 输入格式: YYYYMMDD -> 日期对象
        date_obj = pd.to_datetime(date, format='%Y%m%d')

        # 处理不同的日期类型
        if isinstance(df['date'].iloc[0], str):
            # 字符串格式，比较 date part
            mask = (pd.to_datetime(df['date']).dt.date == date_obj.date())
        else:
            # Timestamp 格式，直接比较
            mask = (df['date'].dt.date == date_obj.date())
    except:
        # 如果无法解析，尝试字符串匹配
        mask = df['date'].astype(str).str.contains(date[:4] + '-' + date[4:6] + '-' + date[6:8])

    # 筛选连板天数
    if min_days > 0:
        mask &= (df['consecutive_limit_up_days'] >= min_days)
    if max_days is not None:
        mask &= (df['consecutive_limit_up_days'] <= max_days)

    result = df[mask].sort_values('consecutive_limit_up_days', ascending=False)

    if len(result) == 0:
        console.print(f"[yellow]日期 {date} 没有符合条件的连板股票[/yellow]")
        return

    # 显示结果
    console.print(f"\n[cyan][bold]日期 {date} 连板股票查询结果[/bold][/cyan]")
    console.print(f"共 [green]{len(result)}[/green] 只股票\n")

    # 使用 Rich 表格展示
    table = Table()
    table.add_column("代码", style="cyan", width=12)
    table.add_column("名称", style="white", width=12)
    table.add_column("连板天数", style="magenta", width=8)
    table.add_column("收盘价", style="green", width=8)
    table.add_column("连板类型", style="yellow", width=8)
    table.add_column("次日涨跌幅", width=12)

    for _, row in result.head(30).iterrows():
        board_type = row['board_type']  # 可能是字符串或数字
        next_day_change = f"{row['next_day_open_change_pct']:.2f}%" if pd.notna(row['next_day_open_change_pct']) else '--'
        table.add_row(
            row['symbol'],
            row['name'],
            str(row['consecutive_limit_up_days']),
            f"{row['close']:.2f}",
            str(board_type),
            next_day_change
        )

    console.print(table)

    if len(result) > 30:
        console.print(f"\n... 还有 {len(result) - 30} 只股票未显示")


def search_stock(keyword: str):
    """搜索股票（代码或名称）"""
    df = data_loader.load_ladder_data()
    if df is None:
        return

    # 筛选涨停股票
    df = df[df['is_limit_up'] == True]

    # 获取最近日期
    latest_date = df['date'].max()

    # 搜索
    mask = (df['symbol'].str.contains(keyword, case=False, na=False) |
            df['name'].str.contains(keyword, case=False, na=False))

    # 比较日期（处理 Timestamp 格式）
    if hasattr(latest_date, 'date'):
        mask &= (df['date'].dt.date == latest_date.date())
    else:
        mask &= (df['date'] == latest_date)

    result = df[mask]

    if len(result) == 0:
        console.print(f"[yellow]未找到关键词 '{keyword}' 相关的连板股票[/yellow]")
        return

    # 获取该股票的历史连板数据
    stock_data = data_loader.load_ladder_data()
    stock_data = stock_data[stock_data['symbol'] == result.iloc[0]['symbol']]
    stock_data = stock_data[stock_data['is_limit_up'] == True]
    stock_data = stock_data.sort_values('date')

    console.print(f"\n[cyan][bold]股票搜索结果: {result.iloc[0]['name']} ({result.iloc[0]['symbol']})[/bold][/cyan]\n")

    table = Table()
    table.add_column("日期", width=12)
    table.add_column("连板天数", style="magenta", width=10)
    table.add_column("收盘价", style="green", width=10)
    table.add_column("涨停价", width=10)
    table.add_column("连板类型", style="yellow", width=8)
    table.add_column("次日涨跌幅", width=12)

    for _, row in stock_data.iterrows():
        board_type = row['board_type']
        next_day_change = f"{row['next_day_open_change_pct']:.2f}%" if pd.notna(row['next_day_open_change_pct']) else '--'
        table.add_row(
            str(row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else row['date']),
            str(row['consecutive_limit_up_days']),
            f"{row['close']:.2f}",
            f"{row['limit_price']:.2f}",
            str(board_type),
            next_day_change
        )

    console.print(table)


# ==================== 统计命令 ====================
def show_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """显示连板统计数据"""
    df = data_loader.load_ladder_data()
    if df is None:
        return

    # 筛选涨停股票
    df = df[df['is_limit_up'] == True]

    # 筛选日期范围
    if start_date or end_date:
        mask = pd.Series(True, index=df.index)
        if start_date:
            try:
                start = pd.to_datetime(start_date, format='%Y%m%d')
                mask &= (df['date'] >= start)
            except:
                pass
        if end_date:
            try:
                end = pd.to_datetime(end_date, format='%Y%m%d')
                mask &= (df['date'] <= end)
            except:
                pass
        df = df[mask]

    console.print("\n[cyan][bold]连板统计[/bold][/cyan]\n")

    # 基本统计
    total_records = len(df)
    unique_stocks = df['symbol'].nunique()
    date_range = df['date'].min(), df['date'].max()

    console.print(f"[bold]数据概况:[/bold]")
    console.print(f"  总记录数: {total_records:,}")
    console.print(f"  股票数量: {unique_stocks}")
    date_min_str = date_range[0].strftime('%Y-%m-%d') if hasattr(date_range[0], 'strftime') else str(date_range[0])
    date_max_str = date_range[1].strftime('%Y-%m-%d') if hasattr(date_range[1], 'strftime') else str(date_range[1])
    console.print(f"  日期范围: {date_min_str} ~ {date_max_str}\n")

    # 各连板天数统计
    board_level_counts = df['consecutive_limit_up_days'].value_counts().sort_index()
    console.print(f"[bold]连板天数分布:[/bold]")
    for days, count in board_level_counts.items():
        console.print(f"  {days}连板: [green]{count}[/green] 次")

    # 连板类型统计（board_type 可能是字符串）
    board_type_counts = df['board_type'].value_counts()
    console.print(f"\n[bold]涨停板类型:[/bold]")
    for board_type, count in board_type_counts.items():
        console.print(f"  {board_type}: {count} 次")

    # 次日表现统计
    next_day_stats = df[df['next_day_open_change_pct'].notna()]['next_day_open_change_pct']
    console.print(f"\n[bold]次日开盘表现:[/bold]")
    console.print(f"  平均涨跌幅: [green]{next_day_stats.mean():.2f}%[/green]")
    console.print(f"  正收益比例: {len(next_day_stats[next_day_stats > 0]) / len(next_day_stats) * 100:.1f}%")
    console.print(f"  最大涨幅: [green]{next_day_stats.max():.2f}%[/green]")
    console.print(f"  最大跌幅: [red]{next_day_stats.min():.2f}%[/red]")


# ==================== 趋势分析 ====================
def show_trend(days: int = 7):
    """显示近期连板趋势"""
    df = data_loader.load_ladder_data()
    if df is None:
        return

    # 筛选涨停股票
    df = df[df['is_limit_up'] == True]

    # 获取最近 N 天
    latest_date = df['date'].max()
    all_dates = sorted(df['date'].unique(), reverse=True)[:days]

    console.print(f"\n[cyan][bold]最近 {days} 个交易日连板趋势[/bold][/cyan]\n")

    table = Table()
    table.add_column("日期", width=12)
    table.add_column("总涨停数", style="cyan", width=10)
    table.add_column("首板", style="white", width=8)
    table.add_column("2连板", width=8)
    table.add_column("3连板", style="yellow", width=8)
    table.add_column("4连板+", style="magenta", width=10)

    for date in all_dates:
        date_data = df[df['date'] == date]
        level_counts = date_data['consecutive_limit_up_days'].value_counts()

        date_str = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)
        table.add_row(
            date_str,
            str(len(date_data)),
            str(level_counts.get(1, 0)),
            str(level_counts.get(2, 0)),
            str(level_counts.get(3, 0)),
            str(sum([level_counts.get(d, 0) for d in level_counts.index if d >= 4]))
        )

    console.print(table)


# ==================== 导出功能 ====================
def export_data(date: str, output: str):
    """导出指定日期的数据到 CSV"""
    df = data_loader.load_ladder_data()
    if df is None:
        return

    # 将日期转换为合适的格式
    try:
        date_obj = pd.to_datetime(date, format='%Y%m%d')
        if isinstance(df['date'].iloc[0], str):
            date_str = date_obj.strftime('%Y-%m-%d')
            result = df[df['date'] == date_str]
        else:
            result = df[df['date'] == date_obj]
    except:
        # 如果无法解析，尝试直接匹配
        result = df[df['date'].astype(str).str.contains(date)]

    # 只导出涨停股票
    result = result[result['is_limit_up'] == True]

    if len(result) == 0:
        console.print(f"[yellow]日期 {date} 没有数据[/yellow]")
        return

    output_path = Path(output)
    result.to_csv(output_path, index=False, encoding='utf-8-sig')
    console.print(f"[green]已导出 {len(result)} 条记录到 {output_path}[/green]")


# ==================== 交互式模式 ====================
def interactive_mode():
    """交互式模式"""
    console.print(Panel.fit(
        "[cyan][bold]连板分析交互模式[/bold][/cyan]\n"
        "输入命令进行操作，输入 'help' 查看帮助，'exit' 退出",
        title="欢迎使用",
        border_style="cyan"
    ))

    while True:
        try:
            cmd = console.input("\n[bold cyan]lb> [/bold cyan]").strip()

            if not cmd:
                continue

            if cmd.lower() in ['exit', 'quit', 'q']:
                console.print("[yellow]再见![/yellow]")
                break

            if cmd.lower() == 'help':
                show_help()
            elif cmd.lower() == 'stats':
                show_stats()
            elif cmd.lower() == 'trend':
                show_trend()
            elif cmd.lower().startswith('query '):
                parts = cmd.split()
                if len(parts) >= 2:
                    query_limit_up(parts[1])
                else:
                    console.print("[yellow]用法: query <日期>[/yellow]")
            elif cmd.lower().startswith('search '):
                parts = cmd.split(maxsplit=1)
                if len(parts) >= 2:
                    search_stock(parts[1])
                else:
                    console.print("[yellow]用法: search <股票代码或名称>[/yellow]")
            elif cmd.lower().startswith('export '):
                parts = cmd.split()
                if len(parts) >= 3:
                    export_data(parts[1], parts[2])
                else:
                    console.print("[yellow]用法: export <日期> <输出文件>[/yellow]")
            else:
                console.print(f"[red]未知命令: {cmd}[/red]")
                console.print("输入 'help' 查看可用命令")

        except KeyboardInterrupt:
            console.print("\n[yellow]使用 'exit' 退出[/yellow]")
        except Exception as e:
            console.print(f"[red]错误: {e}[/red]")


def show_help():
    """显示帮助信息"""
    help_text = """
[bold]可用命令:[/bold]

  [cyan]query <日期>[/cyan]     查询指定日期的涨停股票
  [cyan]search <关键词>[/cyan]  搜索股票（代码或名称）
  [cyan]stats[/cyan]            显示统计数据
  [cyan]trend[/cyan]            显示近期趋势
  [cyan]export <日期> <文件>[/cyan]  导出数据到 CSV
  [cyan]help[/cyan]             显示此帮助
  [cyan]exit[/cyan]             退出交互模式

[bold]日期格式:[/bold]  YYYYMMDD，如 20240101

[bold]示例:[/bold]
  query 20241225
  search 涨停
  stats
  trend
  export 20241225 mydata.csv
"""
    console.print(help_text)


# ==================== 命令行入口 ====================
def main():
    """主入口"""
    parser = argparse.ArgumentParser(
        description='连板股票分析 CLI 工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  python cli.py query 20250101                    # 查询指定日期涨停
  python cli.py query 20250101 --min-days 2     # 查询2连板及以上
  python cli.py search 平安银行                   # 搜索股票
  python cli.py stats                             # 显示统计
  python cli.py trend --days 10                   # 显示10天趋势
  python cli.py export 20250101 output.csv        # 导出数据
  python cli.py interactive                       # 进入交互模式
        '''
    )

    subparsers = parser.add_subparsers(dest='command', help='可用命令')

    # query 命令
    query_parser = subparsers.add_parser('query', help='查询指定日期的涨停股票')
    query_parser.add_argument('date', help='日期 (YYYYMMDD)')
    query_parser.add_argument('--min-days', type=int, default=1, help='最小连板天数 (默认: 1)')
    query_parser.add_argument('--max-days', type=int, help='最大连板天数')

    # search 命令
    search_parser = subparsers.add_parser('search', help='搜索股票')
    search_parser.add_argument('keyword', help='股票代码或名称关键词')

    # stats 命令
    stats_parser = subparsers.add_parser('stats', help='显示统计信息')
    stats_parser.add_argument('--start-date', help='开始日期')
    stats_parser.add_argument('--end-date', help='结束日期')

    # trend 命令
    trend_parser = subparsers.add_parser('trend', help='显示连板趋势')
    trend_parser.add_argument('--days', type=int, default=7, help='显示天数 (默认: 7)')

    # export 命令
    export_parser = subparsers.add_parser('export', help='导出数据')
    export_parser.add_argument('date', help='日期 (YYYYMMDD)')
    export_parser.add_argument('output', help='输出文件路径')

    # interactive 命令
    subparsers.add_parser('interactive', help='进入交互模式')

    args = parser.parse_args()

    # 无命令时显示帮助
    if args.command is None:
        parser.print_help()
        parser.exit()

    # 执行命令
    try:
        if args.command == 'query':
            query_limit_up(args.date, args.min_days, args.max_days)
        elif args.command == 'search':
            search_stock(args.keyword)
        elif args.command == 'stats':
            show_stats(args.start_date, args.end_date)
        elif args.command == 'trend':
            show_trend(args.days)
        elif args.command == 'export':
            export_data(args.date, args.output)
        elif args.command == 'interactive':
            interactive_mode()
    except KeyboardInterrupt:
        console.print("\n[yellow]操作已取消[/yellow]")
        sys.exit(0)
    except Exception as e:
        console.print(f"[red]错误: {e}[/red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
