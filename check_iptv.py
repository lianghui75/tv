import asyncio
import aiohttp
import re
import time

# 配置
INPUT_FILE = "tv.txt"
OUTPUT_FILE = "tv_清洗后.txt"
TIMEOUT = 8  # 超时秒数
CONCURRENCY = 20  # 并发数

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Referer": "http://www.github.com/" # 伪Referer防部分基础防盗链
}

async def check_stream(session, url, semaphore):
    async with semaphore:
        try:
            # 仅请求头部或极小部分数据以判断连通性
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=TIMEOUT), allow_redirects=True) as resp:
                if resp.status == 200:
                    # 对于m3u8，进一步检查内容是否包含EXTINF或ts片段
                    if ".m3u8" in url:
                        text = await resp.text()
                        if "#EXTM3U" in text or "#EXTINF" in text or ".ts" in text:
                            return True, "200 OK (Valid M3U8)"
                        return False, "200 OK (Invalid Content)"
                    return True, f"200 OK ({resp.content_type})"
                return False, f"HTTP {resp.status}"
        except asyncio.TimeoutError:
            return False, "Timeout"
        except Exception as e:
            return False, str(e)[:30]

async def main():
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    semaphore = asyncio.Semaphore(CONCURRENCY)
    async with aiohttp.ClientSession() as session:
        tasks = []
        valid_lines = []
        
        print(f"开始检测 {len(lines)} 条线路...")
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                valid_lines.append(line)
                continue
            
            parts = line.split(",")
            if len(parts) >= 2:
                url = parts[1].split("$")[0] # 去除$备注
                tasks.append((line, url))

        results = await asyncio.gather(*(check_stream(session, url, semaphore) for _, url in tasks))
        
        alive_count = 0
        for (line, url), (is_alive, reason) in zip(tasks, results):
            if is_alive:
                valid_lines.append(line)
                alive_count += 1
            else:
                print(f"[-] 失效: {url[:40]}... | 原因: {reason}")

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write("\n".join(valid_lines))
    
    print(f"\n✅ 检测完成！保留 {alive_count} 条，剔除 {len(tasks) - alive_count} 条。已保存至 {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())