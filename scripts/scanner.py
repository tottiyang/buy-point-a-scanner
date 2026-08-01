#!/usr/bin/env python3
"""
买点A全市场扫描器
右侧交易「突破回踩确认」策略

用法:
  # 终端输出
  python3 scripts/scanner.py

  # 带飞书表格写入
  python3 scripts/scanner.py --feishu-sheet <sheet_token>

  # 自定义参数
  python3 scripts/scanner.py --min-cap 80 --max-cap 600
"""

import akshare as ak
import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os
import json
import argparse
import uuid

# ═══════════════════════════════════════════
#  解析参数
# ═══════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(description='买点A/B/C/D 右侧交易全市场扫描器 (B=洗盘反转/破底翻)')
    # 模式
    p.add_argument('--scan-mode', default='A', choices=['A', 'B', 'C', 'D', 'X'], help='扫描模式: A=突破回踩, B=洗盘反转, C=底部首次突破, D=卖压衰竭(缩量预警), X=净流入滞涨(净特大>净大单>0,涨幅2%~5%,换手率>5%,量比>1.3)')
    # 飞书配置
    p.add_argument('--feishu-sheet', default='', help='飞书表格ID（留空则从config.json自动读取）')
    p.add_argument('--feishu-token', default='~/.qclaw/skills-config/feishu/tokens/user_token.json',
                   help='飞书token文件路径')
    # 市值过滤
    p.add_argument('--min-price', type=float, default=10.0, help='最低股价')
    p.add_argument('--min-cap', type=float, default=80.0, help='最小流通市值(亿)')
    p.add_argument('--max-cap', type=float, default=99999, help='最大流通市值(亿)')
    # 买点A核心参数
    p.add_argument('--breakout-gain', type=float, default=5.0, help='买点A: 突破日最小涨幅%')
    p.add_argument('--breakout-vol', type=float, default=1.5, help='买点A: 突破日最小量比')
    p.add_argument('--vol-shrink', type=float, default=0.55, help='买点A: 今日量/突破日量上限')
    p.add_argument('--body-pct', type=float, default=4.0, help='买点A: 今日K线最大实体%')
    p.add_argument('--pullback-min', type=int, default=2, help='买点A: 最少回调天数')
    p.add_argument('--pullback-max', type=int, default=7, help='买点A: 最多回调天数')
    # 买点C核心参数
    p.add_argument('--c-drop-pct', type=float, default=25.0, help='买点C: 从最高点回撤幅度%')
    p.add_argument('--c-breakout-gain', type=float, default=3.5, help='买点C: 突破日最小涨幅%')
    p.add_argument('--c-breakout-vol', type=float, default=1.3, help='买点C: 突破日最小量比')
    p.add_argument('--c-vol-shrink', type=float, default=0.6, help='买点C: 回调日量/突破日量上限')
    p.add_argument('--c-body-pct', type=float, default=4.0, help='买点C: 今日K线最大实体%')
    p.add_argument('--c-pullback-max', type=int, default=5, help='买点C: 最多回调天数')
    # 买点C: 筑底参数
    p.add_argument('--c-base-days', type=int, default=20, help='买点C: 筑底观察天数')
    p.add_argument('--c-avg-body-pct', type=float, default=5.0, help='买点C: 筑底期日均K线实体上限%')
    p.add_argument('--c-base-vol-ratio', type=float, default=1.2, help='买点C: 筑底期均量/长周期均量上限')
    # 买点D核心参数（卖压衰竭预警）
    p.add_argument('--d-trend', type=str, default='ma20ma55', choices=['ma20','ma20ma55'], help='买点D: 趋势基准 ma20=MA20向上 ma20ma55=MA20>MA55金叉')
    p.add_argument('--d-drop-pct', type=float, default=8.0, help='买点D: 从20日高点最小回撤%')
    p.add_argument('--d-vol-ratio20max', type=float, default=0.50, help='买点D: 今日量/20日最高量上限')
    p.add_argument('--d-body-pct', type=float, default=4.0, help='买点D: K线最大实体%')
    p.add_argument('--d-close-above-ma55', action='store_true', default=True, help='买点D: 收盘在MA55上方')
    # 买点X核心参数（净特大滞涨）
    p.add_argument('--x-ratio-min', type=float, default=1.5, help='买点X: 净特大/净大单最小比值(排除1~1.5模糊区)')
    p.add_argument('--x-ratio-max', type=float, default=3.0, help='买点X: 净特大/净大单最大比值(排除>3追高入场)')
    # 买点B核心参数（洗盘反转/破底翻）
    p.add_argument('--e-drop-pct', type=float, default=10.0, help='买点B: 从高点最小回撤%')
    p.add_argument('--e-vol-up-gain', type=float, default=5.0, help='买点B: 放量上涨日最小涨幅%')
    p.add_argument('--e-vol-up-ratio', type=float, default=1.5, help='买点B: 放量上涨日最小量比')
    p.add_argument('--e-vol-shrink-ratio', type=float, default=0.55, help='买点B: 缩量日量/放量上涨日量上限(单日锚点)')
    p.add_argument('--e-reversal-vol-ratio', type=float, default=1.3, help='买点B: 反包日量/缩量日量下限')
    p.add_argument('--e-lookback', type=int, default=15, help='买点B: 搜索缩量日的最大回看天数')
    p.add_argument('--e-cumul-vol-ratio', type=float, default=1.2, help='买点B: 3日累计放量备选-累计量/10日均量×3的下限')
    p.add_argument('--e-cumul-gain', type=float, default=8.0, help='买点B: 3日累计放量备选-3日累计涨幅下限%%')
    # 性能
    p.add_argument('--max-workers', type=int, default=20, help='并行线程数')
    p.add_argument('--hist-days', type=int, default=30, help='买点A: 获取K线天数')
    return p.parse_args()


# ═══════════════════════════════════════════
#  腾讯证券API
# ═══════════════════════════════════════════

def to_gid(code):
    s = str(code).strip()
    if s.startswith(('sh', 'sz', 'bj')):
        return s
    s = s.zfill(6)
    return f"sh{s}" if s.startswith(('6', '9')) else f"sz{s}"


def get_kline(code, days=30):
    gid = to_gid(code)
    url = f'http://ifzq.gtimg.cn/appstock/app/fqkline/get?param={gid},day,,,{days},qfq'
    try:
        r = requests.get(url, timeout=8, headers={'User-Agent': 'Mozilla/5.0'})
        data = r.json()
        if data.get('code') != 0:
            return None
        klines = data.get('data', {}).get(gid, {}).get('qfqday', [])
        if not klines or len(klines) < 12:
            return None
        return [{
            'date': k[0], 'open': float(k[1]), 'close': float(k[2]),
            'high': float(k[3]), 'low': float(k[4]), 'volume': float(k[5]),
        } for k in klines]
    except:
        return None


def get_market_cap(code, timeout=5):
    """获取流通市值（亿）"""
    try:
        gid = to_gid(code)
        r = requests.get(f'http://qt.gtimg.cn/q={gid}',
                         timeout=timeout, headers={'User-Agent': 'Mozilla/5.0'})
        parts = r.text.split('~')
        if len(parts) > 46:
            return float(parts[44])
    except:
        pass
    return 0


def get_turnover_rate(code, timeout=5):
    """获取腾讯API换手率（%），qt.gtimg.cn 字段38
    盘中自动按交易时间缩放估算全天换手率
    """
    try:
        gid = to_gid(code)
        r = requests.get(f'http://qt.gtimg.cn/q={gid}',
                         timeout=timeout, headers={'User-Agent': 'Mozilla/5.0'})
        parts = r.text.split('~')
        if len(parts) > 38:
            rate = float(parts[38])
            # 盘中按已交易时间比例缩放
            now = datetime.now()
            cur_min = now.hour * 60 + now.minute
            market_open = 9 * 60 + 30
            market_close = 15 * 60
            if now.weekday() < 5 and market_open <= cur_min <= market_close:
                passed_w = _calc_passed_weight()
                if 0 < passed_w < 1.0:
                    rate = rate / passed_w
            return rate
    except:
        pass
    return 0


def get_eastmoney_secid(code):
    """Convert stock code to East Money secid format"""
    s = str(code).strip()
    s = s.replace('sh', '').replace('sz', '').replace('bj', '').zfill(6)
    return f"1.{s}" if s.startswith(('6', '9')) else f"0.{s}"


def get_money_flow_b(code):
    """获取东方财富大单/特大单(超大单)当日资金流向

    Returns:
        (big_net, super_big_net)  大单净流入, 超大单净流入（万元）
        None if failed
    """
    try:
        secid = get_eastmoney_secid(code)
        url = f'http://push2.eastmoney.com/api/qt/stock/fflow/daykline/get?secid={secid}&fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65'
        r = requests.get(url, timeout=5, headers={'User-Agent': 'Mozilla/5.0'})
        data = r.json()
        if data.get('data'):
            klines = data['data'].get('klines', [])
            if klines:
                parts = klines[-1].split(',')
                # [0]date [1]主力净 [2]小单 [3]中单 [4]大单 [5]超大单 [6-8]占比...
                return (float(parts[4]), float(parts[5]))
    except:
        pass
    return None


def _calc_macd(kline):
    """计算MACD柱(DIF-DEA)×2 返回与kline对齐的列表"""
    closes = [k['close'] for k in kline]
    n = len(closes)
    ema12 = [closes[0]]
    ema26 = [closes[0]]
    for i in range(1, n):
        ema12.append(ema12[-1] * 11/13 + closes[i] * 2/13)
        ema26.append(ema26[-1] * 25/27 + closes[i] * 2/27)
    dif = [ema12[i] - ema26[i] for i in range(n)]
    dea = [dif[0]]
    for i in range(1, n):
        dea.append(dea[-1] * 8/10 + dif[i] * 2/10)
    return [2 * (dif[i] - dea[i]) for i in range(n)]


# ═══════════════════════════════════════════
#  扫描核心 - 买点A（突破回踩）
# ═══════════════════════════════════════════

def scan_stock_a(code, name, price, args):
    """扫描单只股票，返回买点A匹配结果"""
    # 市值过滤
    if args.min_cap > 0 or args.max_cap < 9999:
        cap = get_market_cap(code)
        if cap < args.min_cap or cap > args.max_cap:
            return None

    kline = get_kline(code, max(args.hist_days, 60))
    if not kline or len(kline) < 55:
        return None

    n = len(kline)
    t = kline[-1]

    # 过滤涨停跌停板
    today_gain = (t['close'] - kline[-2]['close']) / kline[-2]['close'] * 100
    limit_threshold = 9.5 if not code.startswith(('sh688', 'sz300')) else 19.5
    if abs(today_gain) >= limit_threshold:
        return None

    today_body = abs(t['close'] - t['open']) / max(t['open'], 0.01) * 100
    today_vol = t['volume']

    # MA55中期趋势保护：未跌破55日线
    ma55 = sum(k['close'] for k in kline[-55:]) / 55
    if t['close'] < ma55:
        return None

    # 收盘低于5日线（回踩到位标准，确保已跌破短期均线）
    ma5 = sum(k['close'] for k in kline[-5:]) / 5
    if t['close'] >= ma5:
        return None

    # 20日线方向（软过滤→用于排序优先级，不做硬否决）
    ma20 = sum(k['close'] for k in kline[-20:]) / 20
    ma20_p = sum(k['close'] for k in kline[-21:-1]) / 20
    ma20_up = ma20 > ma20_p

    # 搜索突破日 → 买点A触发
    for i in range(n - 2, max(-1, n - args.pullback_max - 2), -1):
        bk = kline[i]
        prev_close = kline[i - 1]['close'] if i > 0 else bk['open']
        gain = (bk['close'] - prev_close) / prev_close * 100

        # 量比基准：突破日之前10天（不含突破日本身），避免事后污染
        avg_v_pre = sum(kline[j]['volume'] for j in range(max(0, i - 10), i)) / min(10, i)
        if avg_v_pre <= 0:
            continue
        vr = bk['volume'] / avg_v_pre

        if gain < args.breakout_gain or vr < args.breakout_vol:
            continue

        ds = n - 1 - i
        if ds < args.pullback_min:
            continue

        # 回调不破低
        pb_lows = [k['low'] for k in kline[i + 1:n]]
        if not pb_lows or min(pb_lows) < bk['low']:
            continue

        # 今日量缩
        if today_vol > bk['volume'] * args.vol_shrink:
            continue
        # 小K线
        if today_body > args.body_pct:
            continue

        return {
            '代码': code, '名称': name,
            '模式': '买点A✅',
            '突破日': bk['date'][5:],
            '涨幅': f"{gain:.1f}%",
            '回调': f"{ds}天",
            '缩量比': f"{today_vol / bk['volume']:.2f}",
            'K线': f"{today_body:.1f}%",
            '价': t['close'],
            '方向': '↑' if ma20_up else '→',
        }

    # 即将触发（条件放宽1.3~1.5倍）
    for i in range(n - 3, max(-1, n - args.pullback_max - 3), -1):
        bk = kline[i]
        prev_close = kline[i - 1]['close'] if i > 0 else bk['open']
        gain = (bk['close'] - prev_close) / prev_close * 100

        # 量比基准：突破日之前10天（不含突破日本身）
        avg_v_pre = sum(kline[j]['volume'] for j in range(max(0, i - 10), i)) / min(10, i)
        if avg_v_pre <= 0:
            continue
        vr = bk['volume'] / avg_v_pre

        if gain < args.breakout_gain or vr < args.breakout_vol:
            continue

        ds = n - 1 - i
        if ds < args.pullback_min:
            continue

        pb_lows = [k['low'] for k in kline[i + 1:n]]
        if not pb_lows or min(pb_lows) < bk['low']:
            continue

        vol_ok = today_vol <= bk['volume'] * args.vol_shrink * 1.3
        body_ok = today_body <= args.body_pct * 1.5
        if not vol_ok or not body_ok:
            continue

        return {
            '代码': code, '名称': name,
            '模式': '即将触发⏳',
            '突破日': bk['date'][5:],
            '涨幅': f"{gain:.1f}%",
            '回调': f"{ds}天",
            '缩量比': f"{today_vol / bk['volume']:.2f}",
            'K线': f"{today_body:.1f}%",
            '价': t['close'],
            '方向': '↑' if ma20_up else '→',
        }

    return None


# ═══════════════════════════════════════════
#  扫描核心 - 买点B（洗盘反转/破底翻）
# ═══════════════════════════════════════════

def scan_stock_b(code, name, price, args):
    """买点B(新版): 洗盘反转/破底翻

    三阶段: 趋势基底 → 缩量日 → 反包日确认
    - 不设MA20约束，允许假破位
    - 缩量对比近23天内放量上涨日的量
    - 反包对比缩量日的量
    - 盘中模式：从腾讯实时行情取数据，量能预估
    """
    # 获取流通市值（用于市值过滤）和总市值（用于换手率计算）
    cap = get_market_cap(code)
    if cap <= 0:
        return None
    if args.min_cap > 0 or args.max_cap < 9999:
        if cap < args.min_cap or cap > args.max_cap:
            return None

    kline = get_kline(code, 60)
    if not kline or len(kline) < 50:
        return None

    n = len(kline)
    yesterday = kline[-2]

    # ── 判断盘中/盘后  ──
    now = datetime.now()
    cur_min = now.hour * 60 + now.minute
    # 交易日判断（周一~五 9:15-15:00 为盘中）
    market_open = 9 * 60 + 15
    market_close = 15 * 60
    is_intraday = now.weekday() < 5 and market_open <= cur_min <= market_close

    if is_intraday:
        # 盘中模式：取实时行情
        rt = get_realtime_tencent(code)
        if not rt:
            return None
        today_close = rt['price']
        today_open = rt['open']
        today_high = rt['high']
        today_low = rt['low']
        yesterday_close = rt['prev_close']
        today_vol_raw = rt['vol']
        # 预估全天成交量
        passed_w = _calc_passed_weight()
        today_vol = today_vol_raw / passed_w if passed_w >= 0.05 else today_vol_raw
    else:
        # 盘后模式：日K线已完整
        today = kline[-1]
        today_close = today['close']
        today_open = today['open']
        today_high = today['high']
        today_low = today['low']
        yesterday_close = kline[-2]['close']
        today_vol = today['volume']

    # 计算MACD
    macd = _calc_macd(kline)

    # ── 阶段一: 趋势基底 ──

    # MA20 > MA55（金叉）
    ma20 = sum(k['close'] for k in kline[-20:]) / 20
    ma55 = sum(k['close'] for k in kline[-55:]) / 55
    if ma20 <= ma55:
        return None

    # MA55 近20天向上
    ma55_prev = sum(k['close'] for k in kline[-75:-55]) / 20
    if ma55 <= ma55_prev:
        return None

    # MA5 > MA55（短中期多头排列）
    ma5 = sum(k['close'] for k in kline[-5:]) / 5
    if ma5 <= ma55:
        return None

    # 此前在MA20上方运行超过10天
    above_count = 0
    for j in range(n - 30, n - 2):
        ma20_j = sum(kline[k_]['close'] for k_ in range(max(0, j - 19), j + 1)) / min(20, j + 1)
        if kline[j]['close'] > ma20_j:
            above_count += 1
    if above_count < 8:
        return None

    # ── 阶段二: 寻找缩量日（从n-2向前搜索） ──
    high_20 = max(k['close'] for k in kline[-20:])

    shrink_day = None
    shrink_day_idx = None
    vol_up_day = None
    vol_up_day_vol = None
    near_miss_shrink = False  # 缩量比在55%~60%(或累计44%~48%)放宽区间
    near_miss_shrink_max = None  # 记录该区间的实际缩量比上限

    # 从最新日期往前，找第一个满足条件的缩量日+它之前的放量上涨日对
    for j in range(n - 2, max(0, n - args.e_lookback - 1), -1):
        kj = kline[j]

        # 回撤检测：20日高点(close)→当日低点(low)回撤≥阈值
        # 用low回撤包含盘中洗盘，比close口径更能识别真实卖压
        drop = (high_20 - kj['low']) / high_20 * 100
        if drop < args.e_drop_pct:
            continue

        # 在缩量日之前寻找放量上涨日（作为缩量对比的锚点）
        cand_vol_up = None
        cand_vol_up_vol = None
        is_cumul_anchor = False  # 是否使用3日累计放量备选

        # ① 优先：单日放量搜索（原逻辑）
        for v in range(j - 1, max(0, j - 24), -1):
            kv = kline[v]
            prev_close_kv = kline[v - 1]['close'] if v > 0 else kv['open']
            gain = (kv['close'] - prev_close_kv) / prev_close_kv * 100
            if gain < args.e_vol_up_gain or kv['close'] <= kv['open']:
                continue
            avg_v = sum(kline[k_]['volume'] for k_ in range(max(0, v - 10), v)) / min(10, v)
            if avg_v <= 0:
                continue
            # 放量条件：量比≥阈值 或 跳空≥阈值（缺口替补，量不足但跳空也代表买方意愿）
            # 跳空定义为开盘>前日最高，幅度=(O-H前)/H前
            prev_high_kv = kline[v - 1]['high'] if v > 0 else kv['open']
            real_gap = kv['low'] > prev_high_kv
            if kv['volume'] / avg_v < args.e_vol_up_ratio:
                # 缺口替补只在近7天内有效（太远的跳空可能只是虚涨）
                gap_window_days = j - v
                if not real_gap or gap_window_days > 7:
                    continue
                is_cumul_anchor = True
            cand_vol_up = kv
            cand_vol_up_vol = kv['volume']
            break

        # ② 备选：单日锚点不满足缩量比时，尝试3日累计放量
        #    只在近12天搜索（锁定近期积累放量，不回溯过远的单日放量日）
        if cand_vol_up is not None:
            # 单日锚点找到了，先检查缩量比是否达标
            # 缺口替补锚点用收严阈值（同累计锚点：0.55→0.44）
            shrink_check = args.e_vol_shrink_ratio * (0.8 if is_cumul_anchor else 1.0)
            if kj['volume'] <= cand_vol_up_vol * shrink_check:
                # 缩量比达标，直接通过
                pass
            else:
                # 单日锚点缩量比不过关，标记清空由累计替代
                cand_vol_up = None
                cand_vol_up_vol = None
        if cand_vol_up is None:
            # 单日锚点不存在或缩量比不过关 → 尝试3日累计放量
            cumul_search_start = max(0, j - 12)
            for v_end in range(j - 1, cumul_search_start - 1, -1):
                v_start = max(v_end - 2, 0)
                window = kline[v_start:v_end + 1]
                if len(window) < 3:
                    continue
                # 3日必须全阳线
                if any(wk['close'] <= wk['open'] for wk in window):
                    continue
                cumul_vol = sum(wk['volume'] for wk in window)
                # 3日累计涨幅（窗口前1日收盘 → 窗口末收盘）
                if v_start > 0:
                    cumul_gain = (window[-1]['close'] - kline[v_start - 1]['close']) / kline[v_start - 1]['close'] * 100
                else:
                    cumul_gain = 0
                if cumul_gain < args.e_cumul_gain:
                    continue
                avg_10d = sum(kline[k_]['volume'] for k_ in range(max(0, v_start - 10), v_start)) / min(10, v_start)
                if avg_10d <= 0:
                    continue
                cumul_vol_ratio = cumul_vol / (avg_10d * 3)
                if cumul_vol_ratio < args.e_cumul_vol_ratio:
                    continue
                # 选择窗口内最高量日作为锚点
                best = max(window, key=lambda wk: wk['volume'])
                cand_vol_up = best
                cand_vol_up_vol = best['volume']
                is_cumul_anchor = True
                break

        if cand_vol_up is None:
            continue

        # 缩量检测：量 ≤ 放量上涨日量 × 阈值
        shrink_threshold = args.e_vol_shrink_ratio
        if is_cumul_anchor:
            shrink_threshold *= 0.8  # 累计放量锚点收紧缩量比（0.55→0.44）
        if kj['volume'] > cand_vol_up_vol * shrink_threshold:
            # 缩量55%~60%也列入即将触发列表（始终基于原始0.55阈值，不受累计锚点收严影响）
            base_near_miss = cand_vol_up_vol * args.e_vol_shrink_ratio * 1.09
            if kj['volume'] <= base_near_miss:
                near_miss_shrink = True
                near_miss_shrink_max = base_near_miss
            else:
                continue

        # 缩量日当天收盘相对前日涨幅≤1%（下跌或平盘，排除跳空涨停/大涨）
        if j > 0:
            prev_close = kline[j-1]['close']
            shrink_close_gain = (kj['close'] - prev_close) / prev_close * 100
        else:
            shrink_close_gain = 0
        if shrink_close_gain > 1.0:
            continue

        # 找到合格的(缩量日, 放量上涨日)对
        shrink_day = kj
        shrink_day_idx = j
        vol_up_day = cand_vol_up
        vol_up_day_vol = cand_vol_up_vol
        break

    if shrink_day is None or vol_up_day is None:
        return None

    # ── 阶段三: 反包日确认 ──

    # 实际涨幅（基于前收盘，非开盘价）
    gain_today = (today_close - yesterday_close) / yesterday_close * 100

    # 阳线（收盘 > 前收盘即视为上涨）
    if today_close <= yesterday_close:
        return None

    # 双因子反包：收盘≥前日最高，或(今高≥前日最高 AND 今收>昨开=阳吃阴)
    if not (today_close >= yesterday['high'] or (today_high >= yesterday['high'] and today_close > yesterday['open'])):
        return None

    # 放量检测：今日量 ≥ 缩量日量 × 阈值（盘中用预估量）
    # 缺口替补：量不足但真实缺口同样认可（缺口=买方紧迫性，幅度不限）
    if today_vol < shrink_day['volume'] * args.e_reversal_vol_ratio:
        real_gap = today_low > yesterday['high']
        if not real_gap:
            return None

    # MACD双因子：柱翻转(柱>前日) 或 DIF>DEA(柱>0，多头趋势未破坏)
    if len(macd) >= 2 and not (macd[-1] > macd[-2] or macd[-1] > 0):
        return None

    # 站上5日线（不再作为必要条件，仅观察参考）
    # ma5 = sum(k['close'] for k in kline[-5:]) / 5
    # if today_close < ma5:
    #     return None

    # 反包日量 > 10日均量 × 0.8（盘中用预估量）
    # 缺口替补：量不足但真实缺口同样通过
    avg_10d_vol = sum(k['volume'] for k in kline[-10:]) / 10
    if today_vol < avg_10d_vol * 0.8:
        real_gap = today_low > yesterday['high']
        if not real_gap:
            return None

    # 缩量日距今不超过2天（洗盘-反包间隔不宜过长）
    if n - 1 - shrink_day_idx > 2:
        return None

    # ── 过滤：当日最高不创10日新高（卖压衰竭特征，非强势突破） ──
    max_high_10d_back = max(k['high'] for k in kline[-11:-1])  # 不含今日的近10天
    if today_high > max_high_10d_back:
        return None

    # ── 三项检查: 资金净流入 / 量>前5日均量 / 换手率>10% ──
    # 均净流出→淘汰；任一不满足→即将触发⏳；全满足→买点B✅

    # ① 资金净流入
    money_flow = get_money_flow_b(code)
    if money_flow is not None:
        big_net, super_big_net = money_flow
        if big_net <= 0 and super_big_net <= 0:
            return None  # 均净流出，淘汰
        fund_ok = big_net > 0 and super_big_net > 0
    else:
        fund_ok = True  # 接口超时放行

    # ② 量 > 前5日均量
    avg_vol_5d = sum(k['volume'] for k in kline[-6:-1]) / 5
    vol_ok = today_vol > avg_vol_5d

    # ③ 换手率 > 10% (qt.gtimg.cn字段38)
    turnover_rate = get_turnover_rate(code)
    if turnover_rate > 0:
        turnover_ok = turnover_rate > 10
    else:
        turnover_ok = True  # 接口超时放行

    # 收集不满足项（仅对实际执行了检查的条件计数）
    fail_reasons = []
    if near_miss_shrink:
        fail_reasons.append('缩量比偏大')
    if money_flow is not None and not fund_ok:
        fail_reasons.append('资金')
    if not vol_ok:
        fail_reasons.append('量不足')
    if turnover_rate > 0 and not turnover_ok:
        fail_reasons.append('换手')

    if near_miss_shrink:
        mode_tag = '即将触发⏳'  # 缩量比接近阈值，归入触发列表
    elif fund_ok and vol_ok:
        mode_tag = '买点B✅'
    else:
        mode_tag = '即将触发⏳'

    # ── 输出 ──
    today_body = abs(today_close - today_open) / today_open * 100
    shrink_ratio = shrink_day['volume'] / vol_up_day_vol
    reversal_ratio = today_vol / shrink_day['volume']
    days_since_shrink = n - 1 - shrink_day_idx

    return {
        '代码': code, '名称': name,
        '模式': mode_tag,
        '今涨%': f"{gain_today:.1f}%",
        '放量比': f"{reversal_ratio:.2f}",
        '缩量日': shrink_day['date'][5:],
        '缩量比': f"{shrink_ratio:.2f}",
        '缩距': f"{days_since_shrink}天",
        '放量日': vol_up_day['date'][5:],
        '价': today_close,
        'K线': f"{today_body:.1f}%",
        '不满足': ','.join(fail_reasons),
    }


def scan_stock_c(code, name, price, args):
    """扫描单只股票，返回买点C匹配结果

    三阶段: 下跌筑底 → 首次突破 → 回踩确认
    """
    # 市值过滤
    if args.min_cap > 0 or args.max_cap < 9999:
        cap = get_market_cap(code)
        if cap < args.min_cap or cap > args.max_cap:
            return None

    # 买点C需要更多历史数据(150天)来判断下跌筑底
    kline = get_kline(code, 150)
    if not kline or len(kline) < 90:
        return None

    n = len(kline)
    today = kline[-1]

    # ── 阶段一: 下跌筑底 ──

    # 1.1 从120日最高点回撤 >= 30%
    peak_120 = max(k['close'] for k in kline[-120:])
    current = today['close']
    drawdown = (peak_120 - current) / peak_120 * 100
    if drawdown < args.c_drop_pct:
        return None

    # 1.2 筑底期：近c_base_days天窄幅震荡
    base = kline[-args.c_base_days:]
    base_bodies = [abs(k['close'] - k['open']) / max(k['open'], 0.01) * 100 for k in base]
    avg_body = sum(base_bodies) / len(base_bodies)
    if avg_body > args.c_avg_body_pct:
        return None

    # 1.3 地量：筑底期均量 <= 60日均量 * c_base_vol_ratio
    base_avg_vol = sum(k['volume'] for k in base) / len(base)
    long_avg_vol = sum(k['volume'] for k in kline[-60:]) / 60
    if long_avg_vol <= 0 or base_avg_vol / long_avg_vol > args.c_base_vol_ratio:
        return None

    # ── 阶段二: 搜索首次突破日 ──

    breakout_day_idx = None
    breakout_k = None

    # 搜索最近30天内的突破事件
    # 首次突破 = 放量站上20日线 + 此前k天在20日线下方
    search_window = min(30, n - 2)
    for i in range(n - 2, max(0, n - search_window - 1), -1):
        bk = kline[i]
        prev_close = kline[i - 1]['close'] if i > 0 else bk['open']
        gain = (bk['close'] - prev_close) / prev_close * 100

        # 必须是阳线且涨幅达标
        if bk['close'] <= bk['open'] or gain < args.c_breakout_gain:
            continue

        # 计算当时的20日均线
        if i < 19:
            continue
        ma20_at_break = sum(kline[j]['close'] for j in range(i - 19, i + 1)) / 20

        # 收盘站上20日线
        if bk['close'] <= ma20_at_break:
            continue

        # 量比
        vol_10 = sum(kline[j]['volume'] for j in range(i - 10, i)) / 9
        if vol_10 <= 0 or bk['volume'] / vol_10 < args.c_breakout_vol:
            continue

        # 收盘在上半部（买盘强势）
        body_center = (bk['open'] + bk['close']) / 2
        if bk['close'] < body_center:
            continue

        # 首次突破确认：在此之前至少10天都在20日线下方
        prev_below = True
        for j in range(max(0, i - 15), i):
            ma20_j = sum(kline[k_]['close'] for k_ in range(j - 19, j + 1)) / 20
            if kline[j]['close'] > ma20_j:
                prev_below = False
                break

        if not prev_below:
            continue

        breakout_day_idx = i
        breakout_k = bk
        breakout_gain_value = gain
        break

    if breakout_day_idx is None:
        return None

    # 突破日至今的天数
    days_since_breakout = n - 1 - breakout_day_idx
    if days_since_breakout < 1:
        return None  # 突破当天，还没到回踩阶段

    # ── 阶段三: 回踩确认 ──

    # 回调不破20日线（允许盘中瞬间跌破但收盘收回）
    ma20_today = sum(k['close'] for k in kline[-20:]) / 20
    pb_lows = [k['low'] for k in kline[breakout_day_idx + 1:n]]

    # 检查所有回调日的最低价
    for j in range(breakout_day_idx + 1, n):
        kj = kline[j]
        ma20_j = sum(kline[k_]['close'] for k_ in range(j - 19, j + 1)) / 20
        if kj['close'] < ma20_j and kj['low'] < ma20_j:
            # 收盘跌破20日线 = 突破失败
            return None

    # 今天的情况
    today_body = abs(today['close'] - today['open']) / max(today['open'], 0.01) * 100
    today_vol = today['volume']

    # 放量下跌 = 有问题
    if today['close'] < today['open'] and today_vol > breakout_k['volume'] * 0.8:
        return None

    # 触发判断：缩量 + 小K线 + 不破20日线
    vol_ok = today_vol <= breakout_k['volume'] * args.c_vol_shrink
    body_ok = today_body <= args.c_body_pct
    above_ma20 = today['close'] >= ma20_today or (
        today['close'] < ma20_today and today['low'] < ma20_today and today['close'] > ma20_today * 0.99
    )

    mode = '买点C✅' if (vol_ok and body_ok and above_ma20) else '即将触发⏳'

    return {
        '代码': code, '名称': name,
        '模式': mode,
        '突破日': breakout_k['date'][5:],
        '涨幅': f"{breakout_gain_value:.1f}%",
        '回调': f"{days_since_breakout}天",
        '缩量比': f"{today_vol / breakout_k['volume']:.2f}",
        'K线': f"{today_body:.1f}%",
        '价': today['close'],
        '方向': '↓' if today['close'] < today['open'] else '↑',
        '回撤%': f"{drawdown:.0f}%",
    }


# ═══════════════════════════════════════════
#  扫描核心 - 买点D（卖压衰竭反弹/N字突破）
# ═══════════════════════════════════════════

def _calc_passed_weight(now=None):
    """
    计算已过交易时段权重（分段权重推算法）
    用于量能盘中估算。

    权重分段:
      9:30-10:00  权重 25%（开盘期）
      10:00-11:30 权重 30%（上午主交易期）
      11:30-13:00 权重 0%（午间休市，跳过）
      13:00-13:30 权重 15%（下午开盘）
      13:30-14:30 权重 15%（下午主交易期）
      14:30-15:00 权重 15%（尾盘）

    Returns:
        0-1 之间的权重，15:00 时 = 1.0
    """
    if now is None:
        now = datetime.now()
    h, m = now.hour, now.minute
    cur_min = h * 60 + m
    passed = 0.0

    # 9:30-10:00（权重25%）
    if cur_min >= 10 * 60:
        passed += 0.25
    elif cur_min >= 9 * 60 + 30:
        passed += 0.25 * (cur_min - (9 * 60 + 30)) / 30

    # 10:00-11:30（权重30%）
    if cur_min >= 11 * 60 + 30:
        passed += 0.30
    elif cur_min >= 10 * 60:
        passed += 0.30 * (cur_min - 10 * 60) / 90

    # 午间休市（跳过）

    # 13:00-13:30（权重15%）
    if cur_min >= 13 * 60 + 30:
        passed += 0.15
    elif cur_min >= 13 * 60:
        passed += 0.15 * (cur_min - 13 * 60) / 30

    # 13:30-14:30（权重15%）
    if cur_min >= 14 * 60 + 30:
        passed += 0.15
    elif cur_min >= 13 * 60 + 30:
        passed += 0.15 * (cur_min - (13 * 60 + 30)) / 60

    # 14:30-15:00（权重15%）
    if cur_min >= 15 * 60:
        passed += 0.15
    elif cur_min >= 14 * 60 + 30:
        passed += 0.15 * (cur_min - (14 * 60 + 30)) / 30

    return min(passed, 1.0)


def get_realtime_tencent(code):
    """获取腾讯实时行情（含成交量、资金净流入）"""
    try:
        gid = to_gid(code)
        r = requests.get(f'http://qt.gtimg.cn/q={gid}',
                        timeout=5, headers={'User-Agent': 'Mozilla/5.0'})
        parts = r.text.split('~')
        if len(parts) > 46:
            return {
                'price': float(parts[3]),
                'open': float(parts[5]),
                'high': float(parts[33]),
                'low': float(parts[34]),
                'prev_close': float(parts[4]),
                'vol': float(parts[36]),  # 成交量（手）
                'amount': float(parts[37]) * 10000,  # 成交额（元）
            }
    except:
        pass
    return None


def scan_stock_d(code, name, price, args):
    """买点D: 卖压衰竭预警

    核心逻辑：上升趋势中，出现极度缩量小K线 → 卖盘枯竭信号
    → 不等反包确认，缩量当日即标记

    四条件:
    1. 趋势在：MA20向上（或MA20>MA55金叉）
    2. 回撤够：从20日高点跌了足够多
    3. 极度缩量：量<60日均量×阈值，且<近20日最高量×阈值
    4. 小K线：窄幅整理，不是破位
    """
    if args.min_cap > 0:
        cap = get_market_cap(code)
        if cap < args.min_cap:
            return None

    kline = get_kline(code, 80)
    if not kline or len(kline) < 50:
        return None

    n = len(kline)
    t = kline[-1]
    today_close = t['close']
    today_open = t['open']
    today_vol = t['volume']

    # ── 修复: K-line 接口 OHLC=0 但量有值（数据未回填），从实时接口补 ──
    if today_close == 0 and today_vol > 0:
        rt = get_realtime_tencent(code)
        if rt:
            today_close = rt['price']
            today_open = rt['open']
            today_vol = rt['vol']
            t['close'] = today_close
            t['open'] = today_open
            t['volume'] = today_vol

    today_body = abs(today_close - today_open) / max(today_open, 0.01) * 100

    # ── 条件一: 趋势在 ──
    ma20 = sum(k['close'] for k in kline[-20:]) / 20
    ma20_p = sum(k['close'] for k in kline[-21:-1]) / 20
    ma55 = sum(k['close'] for k in kline[-55:]) / 55

    if args.d_trend == 'ma20':
        trend_ok = ma20 > ma20_p
    else:  # ma20ma55
        trend_ok = ma20 > ma55

    if not trend_ok:
        return None

    # ── 条件二: 回撤够（让高位卖盘松动） ──
    high20 = max(k['close'] for k in kline[-20:])
    drop = (high20 - today_close) / high20 * 100
    if drop < args.d_drop_pct:
        return None

    # ── 条件三: 极度缩量（仅对比20日最高量） ──
    vol20_max = max(k['volume'] for k in kline[-20:])
    vol_ratio20max = today_vol / vol20_max if vol20_max > 0 else 999

    if vol_ratio20max > args.d_vol_ratio20max:
        return None

    # ── 条件四: 小K线（窄幅整理，不是破位） ──
    if today_body > args.d_body_pct:
        return None

    # ── 条件五: 剔除跌停板（跌停板缩量不是卖压衰竭，是流动性枯竭） ──
    prev_close = kline[-2]['close']
    drop_pct = (prev_close - today_close) / prev_close * 100
    limit_pct = 19.0 if code.startswith(('sh688','sz300')) else 9.5
    if drop_pct >= limit_pct:
        return None

    # ── 条件六（可选）: 仍在MA55上方（长线趋势未破） ──
    if args.d_close_above_ma55 and today_close < ma55:
        return None

    # ── 条件七: 当日下跌或涨幅低于1%（排除跳空大涨/涨停缩量） ──
    prev_close = kline[-2]['close']
    gain_pct = (today_close - prev_close) / prev_close * 100
    if gain_pct > 1.0:
        return None

    # ── 条件八: 当日量能低于昨日/前日/倒数第3日15%（或的逻辑，防单日跳变干扰） ──
    yesterday_vol = kline[-2]['volume']
    daybefore_vol = kline[-3]['volume'] if len(kline) >= 3 else yesterday_vol
    thirdbefore_vol = kline[-4]['volume'] if len(kline) >= 4 else yesterday_vol
    vol_shrink_yesterday = today_vol < yesterday_vol * 0.85
    vol_shrink_daybefore = today_vol < daybefore_vol * 0.85
    vol_shrink_thirdbefore = today_vol < thirdbefore_vol * 0.85
    if not (vol_shrink_yesterday or vol_shrink_daybefore or vol_shrink_thirdbefore):
        return None

    # ── MACD计算 ──
    macd_list = _calc_macd(kline)
    if not macd_list:
        return None
    curr_macd = macd_list[-1]

    # ── 条件九: MACD柱为负（绿柱，DIFF < DEA） ──
    if curr_macd >= 0:
        return None

    # ── 条件十: 不是近6日最长绿柱（绿柱缩短=底背驰迹象） ──
    macd_window = 6
    window_macd = macd_list[-macd_window:] if len(macd_list) >= macd_window else macd_list
    most_negative = min(window_macd)
    if curr_macd == most_negative:
        return None

    # ── 输出 ──
    vol_ratio_vol5 = today_vol / (sum(k['volume'] for k in kline[-5:]) / 5)
    return {
        '代码': code, '名称': name,
        '模式': '买点D💤',
        '突破日': '-',
        '量/20高': f"{vol_ratio20max:.0%}",
        '量/5均': f"{vol_ratio_vol5:.0%}",
        'K线': f"{today_body:.1f}%",
        '回撤': f"{drop:.0f}%",
        '价': today_close,
        '方向': '💤',
        'MA20': f"{ma20:.0f}",
        'MA55': f"{ma55:.0f}",
        '高': f"{high20:.0f}",
    }


# ═══════════════════════════════════════════
#  扫描核心 - 买点X（主力净流入滞涨）
# ═══════════════════════════════════════════

def scan_stock_x(code, name, price, args):
    """买点X: 净特大>净大单>0，涨幅2%~5%，换手率>5%，量比>1.3

    逻辑:
    1. 资金面: 净特大 > 净大单 > 0 (NeoData验证)
    2. 涨幅: 基于前收盘，2%~5%（滞涨特征）
    3. 量能: 当日量/5日均量 > 1.3（量比>1.3）
    """
    if args.min_cap > 0:
        cap = get_market_cap(code)
        if cap < args.min_cap:
            return None

    # ── 获取K线历史(用于量能对比和涨幅) ──
    kline = get_kline(code, 30)
    if not kline or len(kline) < 2:
        return None

    # ── 判断盘中/盘后 ──
    now = datetime.now()
    cur_min = now.hour * 60 + now.minute
    market_open = 9 * 60 + 15
    market_close = 15 * 60
    is_intraday = now.weekday() < 5 and market_open <= cur_min <= market_close

    if is_intraday:
        rt = get_realtime_tencent(code)
        if not rt:
            return None
        today_close = rt['price']
        prev_close = rt['prev_close']
        today_vol_raw = rt['vol']
        passed_w = _calc_passed_weight()
        today_vol_est = today_vol_raw / passed_w if passed_w >= 0.05 else today_vol_raw
        vol_actual = today_vol_est
    else:
        today = kline[-1]
        today_close = today['close']
        prev_close = kline[-2]['close']
        vol_actual = today['volume']

    # ── 条件2: 涨幅在2%~5%之间 (滞涨但已有启动信号) ──
    gain_pct = (today_close - prev_close) / prev_close * 100
    if gain_pct > 5.0 or gain_pct < 2.0:
        return None

    # ── 条件3: 换手率 > 5%（盘中自动估算）──
    turnover_rate = get_turnover_rate(code)
    if turnover_rate <= 0 or turnover_rate <= 5.0:
        return None

    # ── 条件4: 量比 > 1.3（当日量/5日均量）──
    recent_5_vol = [k['volume'] for k in kline[-6:-1] if k['volume'] > 0]
    if len(recent_5_vol) < 3:
        return None
    avg_5_vol = sum(recent_5_vol) / len(recent_5_vol)
    vol_ratio = vol_actual / avg_5_vol
    if vol_ratio <= 1.3:
        return None

    # ── 条件1: 资金面（净特大 > 净大单 > 0，NeoData验证）──
    super_big, big, source_label = _check_super_greater_than_big(code)
    if super_big is None or big <= 0:
        return None
    ratio = super_big / big

    # 回测结论：比值1~1.5属自然波动(胜率25%)，>3属追高买入(胜率17.2%)
    # 甜区1.5~3胜率52.2%,均值+3.14%
    if ratio < args.x_ratio_min or ratio > args.x_ratio_max:
        return None

    main_net_val = (super_big + big) / 10000  # NeoData字段为元，转万元
    main_net_label = f"{main_net_val:.0f}"

    # ── 涨幅标签 ──
    if gain_pct >= 0:
        gain_label = f"+{gain_pct:.2f}%"
    else:
        gain_label = f"{gain_pct:.2f}%"

    return {
        '代码': code,
        '名称': name,
        '模式': '买点X✅',
        '涨幅%': gain_label,
        '量比': f"{vol_ratio:.2f}",
        '主力净(万)': main_net_label,
        '净特大': f"{super_big/10000:.0f}",
        '净大单': f"{big/10000:.0f}",
        '比值': f"{ratio:.2f}",
        '数据源': source_label,
        '价': today_close,
    }


def _check_super_greater_than_big(code):
    """通过NeoData查询该股**当天**超大单净流入 > 大单净流入 > 0

    盘中模式优先解析「今日资金流向」类型（实时数据），
    盘后/历史数据回退到「历史资金流向」表的最新一行（必须为当天日期）。

    返回: (super_big, big, source_label) 数值（元）+ 数据来源标签
    其中 source_label = '今日' | '历史-今天' | '失败-xxx'
    """
    try:
        import requests as _req
        import re
        today_str = datetime.now().strftime('%Y%m%d')
        std_code = code.replace('sh', '').replace('sz', '').replace('bj', '').zfill(6)
        query_text = f'{std_code}资金流向超大单大单'
        payload = {
            'channel': 'neodata',
            'sub_channel': 'qclaw',
            'query': query_text,
            'request_id': uuid.uuid4().hex,
            'data_type': 'api',
            'se_params': {},
            'extra_params': {},
        }
        proxy_port = os.getenv('AUTH_GATEWAY_PORT', '19000')
        r = _req.post(
            f'http://localhost:{proxy_port}/proxy/api',
            headers={
                'Content-Type': 'application/json',
                'Remote-URL': 'https://jprx.m.qq.com/aizone/skillserver/v1/proxy/teamrouter_neodata/query',
            },
            json=payload,
            timeout=8,
        )
        data = r.json()
        recalls = data.get('data', {}).get('apiData', {}).get('apiRecall', [])

        # ── 优先：今日资金流向（盘中实时，含今日超大单/大单净流入）──
        for recall in recalls:
            if recall.get('type') == '今日资金流向':
                text = recall['content']
                # 格式: "超大单流入29249906元，大单流入24576669元"
                # 此处「流入」实际为净流入值（小单流入可为负）
                m_super = re.search(r'超大单流入(-?[\d,]+\.?\d*)元', text)
                # ⚠️ 负向lookbehind防止匹配到「超大单流入」中的「大单」
                m_big = re.search(r'(?<!超)大单流入(-?[\d,]+\.?\d*)元', text)
                if m_super and m_big:
                    super_big = float(m_super.group(1).replace(',', ''))
                    big = float(m_big.group(1).replace(',', ''))
                    if super_big > big and big > 0:
                        return (super_big, big, '今日')

        # ── 回退：历史资金流向表（⚠️ 仅接受当天日期的行）──
        for recall in recalls:
            if recall.get('type') == '历史资金流向':
                lines = recall['content'].split('\n')
                for line in lines:
                    cols = [c.strip() for c in line.split('|')]
                    # cols[1] = 日期(YYYYMMDD)
                    if len(cols) >= 14 and len(cols[1]) == 8 and cols[1].isdigit():
                        # 🚨 日期守卫：不是今天的数据直接跳过
                        if cols[1] != today_str:
                            continue
                        try:
                            super_big = float(cols[12].replace(',', ''))
                            big = float(cols[13].replace(',', ''))
                            if super_big > big and big > 0:
                                return (super_big, big, '历史-今天')
                            return (None, None, '历史-条件不符')
                        except ValueError:
                            pass
                # 历史表有数据但没有今天这一行 → 没拿到今日数据
                return (None, None, '历史-无今日数据')
        # 今日和历史都没拿到
        return (None, None, '失败-无数据')
    except Exception as e:
        print(f"  [NeoData失败] {code}: {type(e).__name__}")
        return (None, None, '失败-' + type(e).__name__)



def scan_stock(code, name, price, args):
    """根据scan_mode分流"""
    if args.scan_mode == 'X':
        return scan_stock_x(code, name, price, args)
    if args.scan_mode == 'D':
        return scan_stock_d(code, name, price, args)
    if args.scan_mode == 'C':
        return scan_stock_c(code, name, price, args)
    if args.scan_mode == 'B':
        return scan_stock_b(code, name, price, args)
    return scan_stock_a(code, name, price, args)


# ═══════════════════════════════════════════
#  股票池
# ═══════════════════════════════════════════

CACHE_FILE = '/tmp/astock_pool_a.csv'
CACHE_DAYS = 7


def resolve_feishu_sheet(args):
    if args.feishu_sheet:
        return args.feishu_sheet
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')
    if os.path.exists(config_path):
        try:
            with open(config_path) as f:
                cfg = json.load(f)
            mode = args.scan_mode
            if mode in cfg.get('feishu_sheets', {}):
                sheet = cfg['feishu_sheets'][mode]
                if sheet:
                    print(f"  [配置] 自动读取 {mode} 的feishu_sheet")
                    return sheet
        except Exception as e:
            print(f"  [配置] 读取失败: {e}")
    return ''


def load_pool(args):
    if os.path.exists(CACHE_FILE) and \
       time.time() - os.path.getmtime(CACHE_FILE) < 86400 * CACHE_DAYS:
        return pd.read_csv(CACHE_FILE).to_dict('records')

    print("  [缓存] 首次构建...", end=" ", flush=True)
    df = ak.stock_zh_a_spot()
    df = df[~df['代码'].astype(str).str.startswith(('bj', '4', '8'))]
    df = df[~df['名称'].str.contains('ST|退', na=False)]
    df = df[df['最新价'] >= args.min_price]
    df.to_csv(CACHE_FILE, index=False)
    print(f"{len(df)} 只")
    return df.to_dict('records')


# ═══════════════════════════════════════════
#  飞书表格写入
# ═══════════════════════════════════════════

def write_feishu(hits, near, args):
    if not args.feishu_sheet:
        return

    tk_path = os.path.expanduser(args.feishu_token)
    if not os.path.exists(tk_path):
        print("  [飞书] 跳过: token文件不存在")
        return
    try:
        with open(tk_path) as f:
            token_data = json.load(f)
        access_token = token_data.get('access_token')
        if not access_token:
            return
    except:
        return

    today = datetime.now().strftime('%Y%m%d')
    # 无标的则跳过飞书写入
    if not hits and not near:
        print('  (无标的，跳过飞书写入)')
        return
    sample = (hits + near)
    # 检测模式
    is_x = len(sample) > 0 and sample[0].get('主力净(万)') is not None
    is_c = not is_x and len(sample) > 0 and sample[0].get('回撤%') is not None
    is_d = not is_x and len(sample) > 0 and sample[0].get('量/20高') is not None
    is_b = not is_x and len(sample) > 0 and sample[0].get('缩量日') is not None
    if is_x:
        headers = ['代码', '名称', '涨幅%', '量比', '主力净(万)', '净特大', '净大单', '比值', '数据源', '现价', '状态']
    elif is_c:
        headers = ['代码', '名称', '突破日', '涨幅%', '回调天数', '缩量比', 'K线%', '现价', '回撤%', '状态']
    elif is_d:
        headers = ['代码', '名称', '量/20高', '量/5', 'K线', '回撤%', '收盘', '20日高', 'MA20', '方向']
    elif is_b:
        headers = ['代码', '名称', '今涨%', '放量比', '缩量日', '缩量比', '缩距', '放量日', '现价', 'K线%', '不满足', '状态']
    else:
        headers = ['代码', '名称', '突破日', '涨幅%', '回调天数', '缩量比', 'K线%', '现价', '状态']
    sheet_token = args.feishu_sheet

    # 创建sheet
    r = requests.post(
        f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{sheet_token}/sheets_batch_update',
        headers={'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'},
        json={'requests': [{'addSheet': {'properties': {'title': today, 'index': 0}}}]},
        timeout=10
    )

    new_sheet_id = None
    resp = r.json()
    if resp.get('code') == 0:
        new_sheet_id = resp['data']['replies'][0]['addSheet']['properties']['sheetId']
    else:
        # 复用已有sheet
        q = requests.get(
            f'https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{sheet_token}/sheets/query',
            headers={'Authorization': f'Bearer {access_token}'}, timeout=10
        )
        for s in q.json().get('data', {}).get('sheets', []):
            if s['title'] == today:
                new_sheet_id = s['sheet_id']
                break
        if not new_sheet_id:
            print("  [飞书] 跳过: 无法创建或找到sheet")
            return

    def clean(code):
        return code[2:] if code.startswith(('sh', 'sz')) else code

    rows = [headers]
    if is_x:
        extra_cols = ['净特大', '净大单']
    elif is_d:
        extra_cols = ['量/20高', '量/5均', 'MA20', 'MA55']
    elif is_b:
        extra_cols = ['今涨%', '放量比', '缩量日', '缩量比', '缩距', '放量日']
    else:
        extra_cols = ['回撤%'] if is_c else []

    def build_row(r, status_label):
        if is_x:
            row = [clean(r['代码']), r['名称'],
                   r['涨幅%'], r['量比'],
                   r['主力净(万)'],
                   r['净特大'], r['净大单'],
                   r.get('数据源', '-'),
                   f"{r['价']:.2f}", '触发']
        elif is_b:
            not_satisfied = r.get('不满足', '')
            row = [clean(r['代码']), r['名称'],
                   r['今涨%'].replace('%', ''), r['放量比'],
                   r['缩量日'], r['缩量比'], r['缩距'].replace('天', ''),
                   r['放量日'], f"{r['价']:.2f}", r['K线'].replace('%', ''),
                   not_satisfied, status_label]
        elif is_d:
            row = [clean(r['代码']), r['名称'],
                   r['量/20高'],
                   r['量/5均'], r['K线'],
                   r['回撤'], f"{r['价']:.2f}",
                   r['高'], r['MA20'], '💤']
        else:
            row = [clean(r['代码']), r['名称'], r['突破日'],
                   r['涨幅'].replace('%', ''), r['回调'].replace('天', ''),
                   r['缩量比'], r['K线'].replace('%', ''), f"{r['价']:.2f}"]
            for col in extra_cols:
                if col in r:
                    row.append(r[col].replace('%', ''))
            row.append(status_label)
        return row

    # 飞书写入也按放量比降序
    hits_sorted = sorted(hits, key=lambda x: float(x.get('放量比', 0)), reverse=True) if is_b else hits
    near_sorted = sorted(near, key=lambda x: float(x.get('放量比', 0)), reverse=True) if is_b else near

    for r_ in hits_sorted:
        rows.append(build_row(r_, '触发'))
    sep = [''] * (len(headers) - 1) + ['--- 即将触发 ---']
    rows.append(sep)
    rows.append(headers)
    for r_ in near_sorted:
        rows.append(build_row(r_, '即将触发'))

    cols = len(headers)
    # 补空行到200行，覆盖掉底部残留旧数据
    empty_row = [''] * cols
    while len(rows) < 200:
        rows.append(empty_row[:])
    range_str = f"{new_sheet_id}!A1:{chr(64 + cols)}{len(rows)}"
    r2 = requests.put(
        f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{sheet_token}/values',
        headers={'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'},
        json={'valueRange': {'range': range_str, 'values': rows}},
        timeout=15
    )
    resp2 = r2.json()
    if resp2.get('code') == 0:
        print(f"  [飞书] ✅ 已写入sheet「{today}」({resp2['data']['updatedCells']} cells)")


# ═══════════════════════════════════════════
#  主函数
# ═══════════════════════════════════════════

def print_table(items, label):
    if not items:
        return
    is_x = items[0].get('主力净(万)') is not None
    is_b = not is_x and items[0].get('缩量日') is not None
    is_c = not is_x and items[0].get('回撤%') is not None
    is_d = not is_x and items[0].get('量/20高') is not None
    if is_x:
        print(f"\n  {'代码':>7} {'名称':<6} {'涨幅%':<7} {'量比':<6} {'主力净(万)':<10} {'净特大':<10} {'净大单':<10} {'现价':<7}")
        print(f"  {'─'*60}")
        for r in sorted(items, key=lambda x: float(x['量比'].rstrip('x').rstrip('倍')) if 'x' in x['量比'] or '倍' in x['量比'] else float(x['量比']), reverse=True):
            print(f"  {r['代码']:>7} {r['名称']:<6} {r['涨幅%']:<7} {r['量比']:<6} {r['主力净(万)']:<10} {r['净特大']:<10} {r['净大单']:<10} {r['价']:<7.2f}")
    elif is_b:
        print(f"\n  {'代码':>7} {'名称':<6} {'今涨%':<5} {'放量比':<6} {'缩量日':<6} {'缩量比':<5} {'缩距':<4} {'放量日':<6} {'现价':<7} {'K线%':<5} {'不满足':<7}")
        print(f"  {'─'*70}")
        for r in sorted(items, key=lambda x: float(x['放量比']), reverse=True):
            fail_info = r.get('不满足', '')
            print(f"  {r['代码']:>7} {r['名称']:<6} {r['今涨%']:<5} {r['放量比']:<6} {r['缩量日']:<6} {r['缩量比']:<5} {r['缩距']:<4} {r['放量日']:<6} {r['价']:<7.2f} {r['K线']:<5} {fail_info:<7}")
    elif is_c:
        print(f"\n  {'代码':>7} {'名称':<7} {'突破日':<6} {'涨幅':<6} {'回调':<4} {'缩量比':<5} {'K线':<5} {'现价':<7} {'回撤':<5} {'方向'}")
        print(f"  {'─'*57}")
        for r in sorted(items, key=lambda x: float(x['回撤%'].replace('%', '')), reverse=True):
            print(f"  {r['代码']:>7} {r['名称']:<7} {r['突破日']:<6} {r['涨幅']:<6} {r['回调']:<4} {r['缩量比']:<5} {r['K线']:<5} {r['价']:<7.2f} {r['回撤%']:<5} {r['方向']}")
    elif is_d:
        print(f"\n  {'代码':>7} {'名称':<7} {'量/20高':<6} {'量/5':<6} {'K线':<5} {'回撤':<5} {'收盘':<7} {'20日高':<7} {'MA20':<6} {'方向'}")
        print(f"  {'─'*60}")
        for r in sorted(items, key=lambda x: float(x['量/20高'].rstrip('%'))):
            print(f"  {r['代码']:>7} {r['名称']:<7} {r['量/20高']:<6} {r['量/5均']:<6} {r['K线']:<5} {r['回撤']:<5} {r['价']:<7.2f} {r['高']:<7} {r['MA20']:<6} {r['方向']}")
    else:
        print(f"\n  {'代码':>7} {'名称':<7} {'突破日':<6} {'涨幅':<6} {'回调':<4} {'缩量比':<5} {'K线':<5} {'现价':<7} {'MA20'}")
        print(f"  {'─'*48}")
        def _a_sort_key(r):
            # MA20向上 ↑ 优先，再按回调天数降序
            dir_rank = 0 if r['方向'] == '↑' else 1
            try:
                pb = int(r['回调'].replace('天',''))
            except:
                pb = 0
            return (dir_rank, -pb)
        for r in sorted(items, key=_a_sort_key):
            print(f"  {r['代码']:>7} {r['名称']:<7} {r['突破日']:<6} {r['涨幅']:<6} {r['回调']:<4} {r['缩量比']:<5} {r['K线']:<5} {r['价']:<7.2f} {r['方向']}")


def main():
    args = parse_args()

    is_c = args.scan_mode == 'C'
    is_b = args.scan_mode == 'B'
    is_d = args.scan_mode == 'D'
    is_x = args.scan_mode == 'X'
    mode_name = {'A': '买点A', 'B': '买点B', 'C': '买点C', 'D': '买点D(卖压衰竭)', 'X': '买点X(净特大>净大单>0 滞涨)'}[args.scan_mode]

    print("═" * 52)
    print(f"  {mode_name}扫描器 | {datetime.now().strftime('%m-%d %H:%M')}")
    print(f"  市值{args.min_cap}-{args.max_cap}亿 | {args.max_workers}线程")
    print("═" * 52)

    t_start = time.time()
    pool = load_pool(args)
    print(f"\n池: {len(pool)} 只 | 扫描中...")

    hits, near = [], []
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futs = {ex.submit(scan_stock, str(s['代码']), s['名称'], s['最新价'], args): s for s in pool}
        done = 0
        for f in as_completed(futs):
            done += 1
            if done % 400 == 0 or done == len(pool):
                pct = done * 100 // len(pool)
                print(f"  {pct}% ({done}/{len(pool)}) {time.time() - t_start:.0f}s",
                      end="\r" if done < len(pool) else "\n")
            try:
                r = f.result()
                if r:
                    if is_d or is_x:
                        # 买点D/X 暂不分触发/即将触发
                        hits.append(r)
                    else:
                        mode_check = r['模式']
                        if '✅' in mode_check:
                            hits.append(r)
                        elif mode_check == '即将触发⏳':
                            near.append(r)
            except:
                pass

    print(f"\n扫描完成: {time.time() - t_start:.0f}s")
    print(f"\n{'─'*52}")
    print(f"  ✅ {mode_name}触发: {len(hits)} 只")
    if not is_d and not is_x:
        print(f"  ⏳ 即将触发: {len(near)} 只（观察1-2天）")
    print(f"{'─'*52}")

    print_table(hits, '触发')
    if hits:
        if is_x:
            print(f"\n  → 买点X是主力净流入滞涨预警")
            print(f"  → 主力净流入>0说明有资金在吸筹")
            print(f"  → 涨幅2%~5%，已有启动信号但没高潮")
            print(f"  → 换手率>5%，交投活跃")
            print(f"  → 净特大>净大单，机构主导")
            print(f"  → 量能提升说明已经有资金在动")
            print(f"  → 策略: 盘中可追，止损跌破今日低点-3%")
            print(f"  ⚠ 数据源: 腾讯gtimg(主力净)，NeoData(净特大/净大单)")
        elif is_d:
            print(f"\n  → 买点D是卖压衰竭预警（非买入信号）")
            print(f"  → 策略: 信号出现后2-5天内博弈反弹，尾盘搞")
            print(f"  → 止损: 信号日最低-3%")
            print(f"  → 止盈: 第一目标前高, 第二目标+10~+15% | 3天内无量不涨就走")
        elif is_c:
            print(f"\n  → 尾盘买入50% | 止损=突破日最低价-3%")
            print(f"  → 止盈: 第一目标+20%, 第二目标+30~+50%")
        elif is_b:
            print(f"\n  → 尾盘买入50% | 止损=缩量日最低价-3%")
            print(f"  → 止盈: 第一目标前高, 第二目标+15~+20%")
            print(f"  → 反包日确认: 阳线收盘超前高+量≥缩量日×1.3+MACD改善+站上5日线")
        else:
            print(f"\n  → 尾盘买入50% | 止损=突破日阳线实体最低-3%")
    if near and not is_d and not is_x:
        print_table(near, '即将触发')
        if is_c:
            print(f"\n  → 加自选，等缩量+小K线+不破20日线再确认")
        else:
            print(f"\n  → 加自选观察，等明日缩量+小K线再确认")
    if not hits and not near:
        print(f"\n  (今日无符合条件标的)")
    print(f"\n{'─'*52}")

# 确保 --feishu-sheet 已通过 config.json 补全
    args.feishu_sheet = resolve_feishu_sheet(args)
    write_feishu(hits, near, args)


if __name__ == '__main__':
    main()
