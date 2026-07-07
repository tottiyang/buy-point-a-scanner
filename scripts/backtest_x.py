#!/usr/bin/env python3
"""
买点X 批量回测 v3
一次加载K线+资金流，同时评估多个基准日
评估后续3个交易日表现

可用资金流窗口: 20260622~20260703
"""
import sys, os, requests, uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scanner import load_pool, to_gid, parse_args, get_kline

args = parse_args()
args.max_workers = 14

# 所有可用的基准日（需要有≥3个后续交易日）
DATES = [
    ('2026-06-22', '+1.69%大涨日'),
    ('2026-06-23', '-1.14%大跌日'),
    ('2026-06-24', '+0.51%横盘'),
    ('2026-06-25', '+0.41%横盘'),
    ('2026-06-26', '-1.74%黑周五'),
    ('2026-06-29', '+1.17%反弹日'),
    ('2026-06-30', '+0.89%温和上'),
]

STOCK_POOL = load_pool(args)
CODE_KEY, NAME_KEY, PRICE_KEY = '代码', '名称', '最新价'
print(f"股票池: {len(STOCK_POOL)} 只")
print(f"基准日: {len(DATES)} 个 ({DATES[0][0]} ~ {DATES[-1][0]})")
print(f"每只跟踪: 3个交易日\n")

# ═══════════════════════════════════════════
#  NeoData 批量资金流查询（缓存单价）
# ═══════════════════════════════════════════

NEO_CACHE = {}

def get_money_flows(code):
    """一次性拉取该票全部资金流，返回 {date_str: (super_big, big)}"""
    if code in NEO_CACHE:
        return NEO_CACHE[code]
    result = {}
    try:
        std_code = code.replace('sh','').replace('sz','').replace('bj','').zfill(6)
        proxy_port = os.getenv('AUTH_GATEWAY_PORT', '19000')
        payload = {
            'channel': 'neodata','sub_channel': 'qclaw',
            'query': f'{std_code}资金流向超大单大单',
            'request_id': uuid.uuid4().hex,
            'data_type': 'api','se_params': {},'extra_params': {},
        }
        r = requests.post(f'http://localhost:{proxy_port}/proxy/api',
            headers={'Content-Type':'application/json',
                     'Remote-URL':'https://jprx.m.qq.com/aizone/skillserver/v1/proxy/teamrouter_neodata/query'},
            json=payload, timeout=8)
        recalls = r.json().get('data',{}).get('apiData',{}).get('apiRecall',[])
        for recall in recalls:
            if recall.get('type') == '历史资金流向':
                for line in recall['content'].split('\n'):
                    cols = [c.strip() for c in line.split('|')]
                    if len(cols) >= 14:
                        d = cols[1].replace('-','').strip()
                        if len(d) == 8 and d.isdigit():
                            try:
                                sb = float(cols[12].replace(',',''))
                                bg = float(cols[13].replace(',',''))
                                result[d] = (sb, bg)
                            except:
                                pass
    except:
        pass
    NEO_CACHE[code] = result
    return result

# ═══════════════════════════════════════════
#  趋势/位置判断
# ═══════════════════════════════════════════

def get_trend_pos(kline, idx):
    try:
        closes = [kline[j]['close'] for j in range(idx-54, idx+1)]
        ma55 = sum(closes) / 55
        prev_start = sum([kline[j]['close'] for j in range(idx-55, idx)]) / 55
        ma55_up = ma55 > prev_start * 0.995
        today_close = kline[idx]['close']
        if today_close > ma55 and ma55_up:
            trend = '向上'
        elif today_close > ma55:
            trend = '震荡'
        else:
            trend = '向下'
        highs_20 = [kline[idx - j]['high'] for j in range(20) if idx - j >= 0]
        high_20 = max(highs_20)
        dist = (high_20 - today_close) / high_20 * 100
        if dist < 5:
            pos = '高位'
        elif dist > 10:
            pos = '低位'
        else:
            pos = '中位'
        return trend, pos
    except:
        return '未知', '未知'

# ═══════════════════════════════════════════
#  多日期单只回测
# ═══════════════════════════════════════════

def backtest_multi(code, name):
    kline = get_kline(code, 80)  # 多要一些K线覆盖最早日期
    if not kline or len(kline) < 60:
        return []

    # 构建日期→索引映射
    date_idx = {}
    for i, k in enumerate(kline):
        date_idx[k['date']] = i

    money_flows = get_money_flows(code)
    results_list = []

    for scan_date, _ in DATES:
        idx = date_idx.get(scan_date)
        if idx is None or idx < 1:
            continue
        if idx + 3 >= len(kline):
            continue  # 需要至少3个后续交易日

        k_today = kline[idx]
        k_prev = kline[idx-1]
        gain_pct = (k_today['close'] - k_prev['close']) / k_prev['close'] * 100
        if gain_pct > 5.0 or gain_pct < 2.0:
            continue

        # 量比 > 1.3
        vols = [kline[idx-1-j]['volume'] for j in range(5) if idx-1-j >= 0 and kline[idx-1-j]['volume'] > 0]
        if len(vols) < 3:
            continue
        avg5 = sum(vols) / len(vols)
        vol_ratio = k_today['volume'] / avg5
        if vol_ratio <= 1.3:
            continue

        # 资金流
        date_clean = scan_date.replace('-','')
        flow = money_flows.get(date_clean)
        if flow is None:
            continue
        super_big, big = flow

        # 旧规则
        old_ok = (big > 0)
        # 新规则
        new_ok = (super_big > big and big > 0)
        # 比值1.5
        ratio15 = (super_big > big * 1.5 and big > 0) if (big > 0 and super_big > 0) else False

        if not old_ok:
            continue

        trend, pos = get_trend_pos(kline, idx)
        buy_price = k_today['close']

        # 后续表现
        max_up = max_down = 0.0
        for day in range(1, 4):
            k = kline[idx + day]
            up = (k['high'] - buy_price) / buy_price * 100
            dn = (buy_price - k['low']) / buy_price * 100
            if up > max_up: max_up = up
            if dn > max_down: max_down = dn
        final_gain = (kline[idx+3]['close'] - buy_price) / buy_price * 100

        results_list.append({
            'date': scan_date,
            'code': code, 'name': name,
            'gain': gain_pct, 'vol_ratio': vol_ratio,
            'super_big': super_big, 'big': big,
            'ratio': super_big/big if big > 0 else 0,
            'trend': trend, 'pos': pos,
            'old': old_ok, 'new': new_ok, 'r15': ratio15,
            'max_up': max_up, 'max_down': max_down,
            'final': final_gain,
        })

    return results_list


# ═══════════════════════════════════════════
#  执行
# ═══════════════════════════════════════════

total = len(STOCK_POOL)
done = 0
all_results = []  # (date, dict)

with ThreadPoolExecutor(max_workers=14) as pool:
    futures = {}
    for s in STOCK_POOL:
        code = to_gid(s[CODE_KEY])
        name = s[NAME_KEY]
        futures[pool.submit(backtest_multi, code, name)] = code

    for fut in as_completed(futures):
        done += 1
        if done % 500 == 0:
            print(f"  扫描中... {done}/{total}", flush=True)
        try:
            results_list = fut.result()
            for r in results_list:
                all_results.append(r)
        except:
            pass

print(f"\n  完成! 扫描 {done} 只, 共 {len(all_results)} 个信号\n")

# ═══════════════════════════════════════════
#  按日期汇总统计
# ═══════════════════════════════════════════

by_date = defaultdict(list)
for r in all_results:
    by_date[r['date']].append(r)

print("=" * 160)
print("各基准日对比汇总")
print("=" * 160)
header = f"{'基准日':<12} {'大盘':<10} {'旧总数':>6} {'旧胜':>5} {'旧率':>6} {'旧均值':>8} {'新总数':>6} {'新胜':>5} {'新率':>6} {'新均值':>8} {'R15数':>6} {'R15胜':>5} {'R15率':>6} {'R15均值':>8}"
print(header)
print("-" * 160)

grand_old = {'total':0, 'win':0, 'final':0}
grand_new = {'total':0, 'win':0, 'final':0}
grand_r15 = {'total':0, 'win':0, 'final':0}

# 按大盘涨跌排序（最涨→最跌）
dt_sorted = sorted(DATES, key=lambda x: x[0])

for scan_date, label in dt_sorted:
    lst = by_date.get(scan_date, [])
    if not lst:
        continue
    
    # 旧规则（所有）
    old_lst = lst  # 全部都是旧规则触发
    old_win = sum(1 for r in old_lst if r['final'] > 0)
    old_avg = sum(r['final'] for r in old_lst) / len(old_lst)
    old_rate = old_win / len(old_lst) * 100
    
    # 新规则
    new_lst = [r for r in lst if r['new']]
    new_win = sum(1 for r in new_lst if r['final'] > 0)
    new_avg = sum(r['final'] for r in new_lst) / len(new_lst) if new_lst else 0
    new_rate = new_win / len(new_lst) * 100 if new_lst else 0
    
    # R15
    r15_lst = [r for r in lst if r['r15']]
    r15_win = sum(1 for r in r15_lst if r['final'] > 0)
    r15_avg = sum(r['final'] for r in r15_lst) / len(r15_lst) if r15_lst else 0
    r15_rate = r15_win / len(r15_lst) * 100 if r15_lst else 0
    
    grand_old['total'] += len(old_lst)
    grand_old['win'] += old_win
    grand_old['final'] += sum(r['final'] for r in old_lst)
    grand_new['total'] += len(new_lst)
    grand_new['win'] += new_win
    grand_new['final'] += sum(r['final'] for r in new_lst)
    grand_r15['total'] += len(r15_lst)
    grand_r15['win'] += r15_win
    grand_r15['final'] += sum(r['final'] for r in r15_lst)
    
    print(f"{scan_date:<12} {label:<10} {len(old_lst):>6} {old_win:>5} {old_rate:>5.1f}% {old_avg:>+7.2f}% {len(new_lst):>6} {new_win:>5} {new_rate:>5.1f}% {new_avg:>+7.2f}% {len(r15_lst):>6} {r15_win:>5} {r15_rate:>5.1f}% {r15_avg:>+7.2f}%")

print("-" * 160)
gr_old_rate = grand_old['win'] / grand_old['total'] * 100
gr_old_avg = grand_old['final'] / grand_old['total']
gr_new_rate = grand_new['win'] / grand_new['total'] * 100
gr_new_avg = grand_new['final'] / grand_new['total'] if grand_new['total'] else 0
gr_r15_rate = grand_r15['win'] / grand_r15['total'] * 100 if grand_r15['total'] else 0
gr_r15_avg = grand_r15['final'] / grand_r15['total'] if grand_r15['total'] else 0
print(f"{'汇总':<12} {'7日合计':<10} {grand_old['total']:>6} {grand_old['win']:>5} {gr_old_rate:>5.1f}% {gr_old_avg:>+7.2f}% {grand_new['total']:>6} {grand_new['win']:>5} {gr_new_rate:>5.1f}% {gr_new_avg:>+7.2f}% {grand_r15['total']:>6} {grand_r15['win']:>5} {gr_r15_rate:>5.1f}% {gr_r15_avg:>+7.2f}%")
print()

# ═══════════════════════════════════════════
#  分层汇总（跨日） - 新规则
# ═══════════════════════════════════════════

all_new = [r for r in all_results if r['new']]
print("=" * 90)
print("【新规则·跨日分层统计】趋势+位置")
print("=" * 90)

layers = {
    '趋势向上': [r for r in all_new if r['trend'] == '向上'],
    '趋势向下': [r for r in all_new if r['trend'] == '向下'],
    '趋势震荡': [r for r in all_new if r['trend'] == '震荡' or r['trend'] in ('震荡上',)],
    '高位': [r for r in all_new if r['pos'] == '高位'],
    '低位': [r for r in all_new if r['pos'] == '低位'],
    '中位': [r for r in all_new if r['pos'] == '中位'],
    '向上+高位(最危险)': [r for r in all_new if r['trend'] == '向上' and r['pos'] == '高位'],
    '向下+低位(抄底盘)': [r for r in all_new if r['trend'] == '向下' and r['pos'] == '低位'],
    '向下+中位': [r for r in all_new if r['trend'] == '向下' and r['pos'] == '中位'],
}

for name, lst in layers.items():
    if not lst:
        print(f'  {name}: 0只')
        continue
    avg = sum(r['final'] for r in lst) / len(lst)
    w = sum(1 for r in lst if r['final'] > 0)
    l = sum(1 for r in lst if r['final'] <= -3)
    print(f"  {name}: {len(lst):>3}只 | 均值 {avg:>+6.2f}% | 胜率 {w/len(lst)*100:>5.1f}% ({w}胜/{len(lst)-w-l}平/{l}败)")

print()

# ═══════════════════════════════════════════
#  分层汇总：比值
# ═══════════════════════════════════════════

print("=" * 90)
print("【新规则·跨日分层统计】比值区间")
print("=" * 90)

ratio_buckets = [
    ('比值1~1.5', lambda r: 1 <= r['ratio'] < 1.5),
    ('比值1.5~2', lambda r: 1.5 <= r['ratio'] < 2),
    ('比值2~3',   lambda r: 2 <= r['ratio'] < 3),
    ('比值>3',    lambda r: r['ratio'] >= 3),
]

for name, cond in ratio_buckets:
    lst = [r for r in all_new if cond(r)]
    if lst:
        avg = sum(r['final'] for r in lst) / len(lst)
        w = sum(1 for r in lst if r['final'] > 0)
        print(f"  {name}: {len(lst):>3}只 | 均值 {avg:>+6.2f}% | 胜率 {w/len(lst)*100:>5.1f}%")

# ═══════════════════════════════════════════
#  按个股汇总（新规则）
# ═══════════════════════════════════════════

print("\n" + "=" * 120)
print("【连续命中多日的股票 - 新规则】")
print("=" * 120)
stock_stats = defaultdict(list)
for r in all_new:
    stock_stats[(r['code'], r['name'])].append(r)

multi_hit = {k: v for k, v in stock_stats.items() if len(v) >= 2}
for (code, name), lst in sorted(multi_hit.items(), key=lambda x: len(x[1]), reverse=True):
    avg = sum(r['final'] for r in lst) / len(lst)
    w = sum(1 for r in lst if r['final'] > 0)
    dates_str = '/'.join(sorted(set(r['date'] for r in lst)))
    print(f"  {code:<10} {name:<8} {len(lst)}次 | 均值 {avg:>+5.2f}% | 胜率 {w/len(lst)*100:.0f}% | 日期: [{dates_str}]")

print(f"\n  连续≥2次命中: {len(multi_hit)} 只")
