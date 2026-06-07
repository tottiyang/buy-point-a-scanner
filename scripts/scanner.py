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
    p = argparse.ArgumentParser(description='买点A全市场扫描器')
    # 飞书配置
    p.add_argument('--feishu-sheet', default='', help='飞书表格ID（留空不写入）')
    p.add_argument('--feishu-token', default='~/.qclaw/skills-config/feishu/tokens/user_token.json',
                   help='飞书token文件路径')
    # 市值过滤
    p.add_argument('--min-price', type=float, default=10.0, help='最低股价')
    p.add_argument('--min-cap', type=float, default=80.0, help='最小流通市值(亿)')
    p.add_argument('--max-cap', type=float, default=600.0, help='最大流通市值(亿)')
    # 买点A核心参数
    p.add_argument('--breakout-gain', type=float, default=5.0, help='突破日最小涨幅%')
    p.add_argument('--breakout-vol', type=float, default=1.5, help='突破日最小量比')
    p.add_argument('--vol-shrink', type=float, default=0.55, help='今日量/突破日量上限')
    p.add_argument('--body-pct', type=float, default=4.0, help='今日K线最大实体%')
    p.add_argument('--pullback-min', type=int, default=2, help='最少回调天数')
    p.add_argument('--pullback-max', type=int, default=7, help='最多回调天数')
    # 性能
    p.add_argument('--max-workers', type=int, default=20, help='并行线程数')
    p.add_argument('--hist-days', type=int, default=30, help='获取K线天数')
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
#  扫描核心
# ═══════════════════════════════════════════

def scan_stock(code, name, price, args):
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
    print(f"\n  {'代码':>7} {'名称':<7} {'突破日':<6} {'涨幅':<6} {'回调':<4} {'缩量比':<5} {'K线':<5} {'现价':<7} {'方向'}")
    print(f"  {'─'*48}")
    for r in sorted(items, key=lambda x: x['回调'], reverse=True):
        print(f"  {r['代码']:>7} {r['名称']:<7} {r['突破日']:<6} {r['涨幅']:<6} {r['回调']:<4} {r['缩量比']:<5} {r['K线']:<5} {r['价']:<7.2f} {r['方向']}")


def main():
    args = parse_args()

    print("═" * 52)
    print(f"  买点A扫描器 | {datetime.now().strftime('%m-%d %H:%M')}")
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
                if r and r['模式'] == '买点A✅':
                    hits.append(r)
                elif r and r['模式'] == '即将触发⏳':
                    near.append(r)
            except:
                pass

    print(f"\n扫描完成: {time.time() - t_start:.0f}s")
    print(f"\n{'─'*52}")
    print(f"  ✅ 买点A触发: {len(hits)} 只（今日可买入）")
    print(f"  ⏳ 即将触发: {len(near)} 只（观察1-2天）")
    print(f"{'─'*52}")

    print_table(hits, '触发')
    if hits:
        print(f"\n  → 尾盘买入50% | 止损=突破日阳线实体最低-3%")
    if near:
        print_table(near, '即将触发')
        print(f"\n  → 加自选观察，等明日缩量+小K线再确认")
    if not hits and not near:
        print(f"\n  (今日无符合条件标的)")
    print(f"\n{'─'*52}")

    write_feishu(hits, near, args)


if __name__ == '__main__':
    main()
