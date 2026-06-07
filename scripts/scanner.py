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

# ═══════════════════════════════════════════
#  解析参数
# ═══════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(description='买点A/C 右侧交易全市场扫描器')
    # 模式
    p.add_argument('--scan-mode', default='A', choices=['A', 'C'], help='扫描模式: A=突破回踩, C=底部首次突破')
    # 飞书配置
    p.add_argument('--feishu-sheet', default='', help='飞书表格ID（留空不写入）')
    p.add_argument('--feishu-token', default='~/.qclaw/skills-config/feishu/tokens/user_token.json',
                   help='飞书token文件路径')
    # 市值过滤
    p.add_argument('--min-price', type=float, default=10.0, help='最低股价')
    p.add_argument('--min-cap', type=float, default=80.0, help='最小流通市值(亿)')
    p.add_argument('--max-cap', type=float, default=600.0, help='最大流通市值(亿)')
    # 买点A核心参数
    p.add_argument('--breakout-gain', type=float, default=5.0, help='买点A: 突破日最小涨幅%')
    p.add_argument('--breakout-vol', type=float, default=1.5, help='买点A: 突破日最小量比')
    p.add_argument('--vol-shrink', type=float, default=0.55, help='买点A: 今日量/突破日量上限')
    p.add_argument('--body-pct', type=float, default=4.0, help='买点A: 今日K线最大实体%')
    p.add_argument('--pullback-min', type=int, default=2, help='买点A: 最少回调天数')
    p.add_argument('--pullback-max', type=int, default=7, help='买点A: 最多回调天数')
    # 买点C核心参数
    p.add_argument('--c-drop-pct', type=float, default=30.0, help='买点C: 从最高点回撤幅度%')
    p.add_argument('--c-breakout-gain', type=float, default=4.0, help='买点C: 突破日最小涨幅%')
    p.add_argument('--c-breakout-vol', type=float, default=1.5, help='买点C: 突破日最小量比')
    p.add_argument('--c-vol-shrink', type=float, default=0.6, help='买点C: 回调日量/突破日量上限')
    p.add_argument('--c-body-pct', type=float, default=4.0, help='买点C: 今日K线最大实体%')
    p.add_argument('--c-pullback-max', type=int, default=5, help='买点C: 最多回调天数')
    # 买点C: 筑底参数
    p.add_argument('--c-base-days', type=int, default=20, help='买点C: 筑底观察天数')
    p.add_argument('--c-avg-body-pct', type=float, default=4.0, help='买点C: 筑底期日均K线实体上限%')
    p.add_argument('--c-base-vol-ratio', type=float, default=0.6, help='买点C: 筑底期均量/长周期均量上限')
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
    try:
        r = requests.get(f'http://qt.gtimg.cn/q={code}',
                         timeout=timeout, headers={'User-Agent': 'Mozilla/5.0'})
        parts = r.text.split('~')
        if len(parts) > 46:
            return float(parts[44])
    except:
        pass
    return 0


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

    kline = get_kline(code, args.hist_days)
    if not kline or len(kline) < 15:
        return None

    n = len(kline)
    t = kline[-1]

    today_body = abs(t['close'] - t['open']) / max(t['open'], 0.01) * 100
    today_vol = t['volume']

    avg_v = sum(k['volume'] for k in kline[-11:-1]) / 9
    if avg_v <= 0:
        return None

    # 20日线方向
    ma20 = sum(k['close'] for k in kline[-20:]) / 20
    ma20_p = sum(k['close'] for k in kline[-21:-1]) / 20
    ma20_up = ma20 > ma20_p
    if not ma20_up:
        return None

    # 搜索突破日 → 买点A触发
    for i in range(n - 2, max(-1, n - args.pullback_max - 2), -1):
        bk = kline[i]
        gain = (bk['close'] - bk['open']) / bk['open'] * 100
        vr = bk['volume'] / avg_v

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
            '方向': '↑',
        }

    # 即将触发（条件放宽1.3~1.5倍）
    for i in range(n - 3, max(-1, n - args.pullback_max - 3), -1):
        bk = kline[i]
        gain = (bk['close'] - bk['open']) / bk['open'] * 100
        vr = bk['volume'] / avg_v

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
            '方向': '↑',
        }

    return None


# ═══════════════════════════════════════════
#  扫描核心 - 买点C（底部首次突破）
# ═══════════════════════════════════════════

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
        gain = (bk['close'] - bk['open']) / bk['open'] * 100

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
        '涨幅': f"{abs(breakout_k['close'] - breakout_k['open']) / breakout_k['open'] * 100:.1f}%",
        '回调': f"{days_since_breakout}天",
        '缩量比': f"{today_vol / breakout_k['volume']:.2f}",
        'K线': f"{today_body:.1f}%",
        '价': today['close'],
        '方向': '↓' if today['close'] < today['open'] else '↑',
        '回撤%': f"{drawdown:.0f}%",
    }


def scan_stock(code, name, price, args):
    """根据scan_mode分流"""
    if args.scan_mode == 'C':
        return scan_stock_c(code, name, price, args)
    return scan_stock_a(code, name, price, args)


# ═══════════════════════════════════════════
#  股票池
# ═══════════════════════════════════════════

CACHE_FILE = '/tmp/astock_pool_a.csv'
CACHE_DAYS = 7


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
    is_c = len(hits) > 0 and hits[0].get('回撤%') is not None
    if is_c:
        headers = ['代码', '名称', '突破日', '涨幅%', '回调天数', '缩量比', 'K线%', '现价', '回撤%', '状态']
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
    if is_c:
        for r_ in hits:
            rows.append([clean(r_['代码']), r_['名称'], r_['突破日'],
                          r_['涨幅'].replace('%', ''), r_['回调'].replace('天', ''),
                          r_['缩量比'], r_['K线'].replace('%', ''), f"{r_['价']:.2f}",
                          r_['回撤%'].replace('%', ''), '触发'])
        rows.append(['', '', '--- 即将触发 ---', '', '', '', '', '', '', ''])
        rows.append(headers)
        for r_ in near:
            rows.append([clean(r_['代码']), r_['名称'], r_['突破日'],
                          r_['涨幅'].replace('%', ''), r_['回调'].replace('天', ''),
                          r_['缩量比'], r_['K线'].replace('%', ''), f"{r_['价']:.2f}",
                          r_['回撤%'].replace('%', ''), '即将触发'])
    else:
        for r_ in hits:
            rows.append([clean(r_['代码']), r_['名称'], r_['突破日'],
                          r_['涨幅'].replace('%', ''), r_['回调'].replace('天', ''),
                          r_['缩量比'], r_['K线'].replace('%', ''), f"{r_['价']:.2f}", '触发'])
        rows.append(['', '', '--- 即将触发 ---', '', '', '', '', '', ''])
        rows.append(headers)
        for r_ in near:
            rows.append([clean(r_['代码']), r_['名称'], r_['突破日'],
                          r_['涨幅'].replace('%', ''), r_['回调'].replace('天', ''),
                          r_['缩量比'], r_['K线'].replace('%', ''), f"{r_['价']:.2f}", '即将触发'])

    cols = len(headers)
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
    is_c = items[0].get('回撤%') is not None
    if is_c:
        print(f"\n  {'代码':>7} {'名称':<7} {'突破日':<6} {'涨幅':<6} {'回调':<4} {'缩量比':<5} {'K线':<5} {'现价':<7} {'回撤':<5} {'方向'}")
        print(f"  {'─'*57}")
        for r in sorted(items, key=lambda x: float(x['回撤%'].replace('%', '')), reverse=True):
            print(f"  {r['代码']:>7} {r['名称']:<7} {r['突破日']:<6} {r['涨幅']:<6} {r['回调']:<4} {r['缩量比']:<5} {r['K线']:<5} {r['价']:<7.2f} {r['回撤%']:<5} {r['方向']}")
    else:
        print(f"\n  {'代码':>7} {'名称':<7} {'突破日':<6} {'涨幅':<6} {'回调':<4} {'缩量比':<5} {'K线':<5} {'现价':<7} {'方向'}")
        print(f"  {'─'*48}")
        for r in sorted(items, key=lambda x: x['回调'], reverse=True):
            print(f"  {r['代码']:>7} {r['名称']:<7} {r['突破日']:<6} {r['涨幅']:<6} {r['回调']:<4} {r['缩量比']:<5} {r['K线']:<5} {r['价']:<7.2f} {r['方向']}")


def main():
    args = parse_args()

    is_c = args.scan_mode == 'C'
    mode_name = '买点C' if is_c else '买点A'

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
                    if r['模式'] in (f'{mode_name}✅', '买点A✅', '买点C✅'):
                        hits.append(r)
                    elif r['模式'] == '即将触发⏳':
                        near.append(r)
            except:
                pass

    print(f"\n扫描完成: {time.time() - t_start:.0f}s")
    print(f"\n{'─'*52}")
    print(f"  ✅ {mode_name}触发: {len(hits)} 只（今日可买入）")
    print(f"  ⏳ 即将触发: {len(near)} 只（观察1-2天）")
    print(f"{'─'*52}")

    print_table(hits, '触发')
    if hits:
        if is_c:
            print(f"\n  → 尾盘买入50% | 止损=突破日最低价-3%")
            print(f"  → 止盈: 第一目标+20%, 第二目标+30~+50%")
        else:
            print(f"\n  → 尾盘买入50% | 止损=突破日阳线实体最低-3%")
    if near:
        print_table(near, '即将触发')
        if is_c:
            print(f"\n  → 加自选，等缩量+小K线+不破20日线再确认")
        else:
            print(f"\n  → 加自选观察，等明日缩量+小K线再确认")
    if not hits and not near:
        print(f"\n  (今日无符合条件标的)")
    print(f"\n{'─'*52}")

    write_feishu(hits, near, args)


if __name__ == '__main__':
    main()
