#!/bin/bash
#!/bin/bash
# 买点X 盘中扫描 cron wrapper (上午盘)
# 交易日 9:35-11:30，每10分钟运行

TZ=Asia/Shanghai date

DOW=$(TZ=Asia/Shanghai date +%u)
# 1-5 = 周一至周五
if [ "$DOW" -ge 6 ]; then
    echo "非交易日，跳过"
    exit 0
fi

NOW=$(TZ=Asia/Shanghai date +%H%M)
if [ "$NOW" -lt 0935 ] || [ "$NOW" -ge 1130 ]; then
    echo "非交易时段($NOW)，跳过"
    exit 0
fi

echo "=== 开始买点X扫描 ==="
cd ~/.qclaw/skills/buy-point-a-scanner
python3 scripts/scanner.py --scan-mode X --max-workers 14
echo "=== 扫描完毕 ==="
