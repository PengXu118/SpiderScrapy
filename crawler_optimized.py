#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XXX - 最新案例爬虫（优化版）
文件名: XXX

功能：
1. 爬取所有最新案例
2. 获取每个案例的详细信息
3. 分批保存，支持断点续爬
4. 输出Excel和CSV格式

特点：
- 更稳定的错误处理
- 合理的延时设置（避免给服务器造成压力）
- 详细的进度显示
- 自动保存检查点
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os
import json
import re
from urllib.parse import urljoin
from datetime import datetime

# ==================== 配置 ====================
# 更换成目标URL
BASE_URL = "XXX"
LIST_URL = "XXX"

# 更真实的浏览器请求头(使用自己的请求头)
HEADERS = {
    'User-Agent': 'XXX',
    'Accept': 'XXX',
    'Accept-Language': 'XXX',
    'Accept-Encoding': 'XXX',
    'Connection': 'XXX',
    'Upgrade-Insecure-Requests': 'XXX',
    'Cache-Control': 'XXX',
    'Referer': 'XXX',
}

# 延时配置（秒）- 超保守模式，最大程度避免给服务器造成负担
DELAY_REQUEST = (6.0, 10.0)     # 每个请求之间：6-10秒
DELAY_PAGE = (15, 25)           # 每页之间：15-25秒
DELAY_BATCH = (90, 150)         # 每批之间：1.5-2.5分钟
DELAY_ERROR = (120, 180)        # 错误后：2-3分钟
DELAY_CONNECTION_RESET = (300, 480)  # 连接重置后：5-8分钟

# 分批配置
BATCH_SIZE = 20                 # 每20条保存一次
MAX_RETRIES = 2                 # 最大重试次数
CASES_PER_SESSION = 60          # 每60条休息一次
MAX_CONNECTION_RESETS = 3       # 最大连接重置次数

# 分时段配置
ENABLE_TIME_LIMIT = True        # 是否启用时间限制
MAX_RUN_TIME = 7200             # 单次最长运行时间（秒），默认2小时
MIN_RUN_TIME = 3600             # 单次最短运行时间（秒），默认1小时
SUGGEST_REST_TIME = 3600        # 建议休息时间（秒），默认1小时

# 代理配置(配置自己的参数)
KDL_API_URL = 'XXX'
KDL_USERNAME = 'XXX'
KDL_PASSWORD = 'XXX'

# 代理配置
USE_PROXY = True                # 启用代理
PROXY_TYPE = 'kdl'              # 代理类型：'kdl'=XXX, 'manual'=手动配置
PROXY_LIST = []                 # 手动配置的代理列表（PROXY_TYPE='manual'时使用）
PROXY_ROTATION = True           # 是否轮换代理
PROXY_REFRESH_INTERVAL = 3000   # 代理刷新间隔（秒），50分钟（避免超限：150分钟内最多3次=9个IP<20限制）
PROXY_RETRY_DELAY = 600         # 代理获取失败后等待时间（秒），10分钟
current_proxy_index = 0         # 当前代理索引
last_proxy_refresh = 0          # 上次刷新代理的时间
proxy_fetch_failed_time = 0     # 代理获取失败的时间

# 全局计数器
connection_reset_count = 0
server_error_count = 0          # 500错误计数器
start_time = None               # 运行开始时间
proxy_fetch_failed_time = 0     # 代理获取失败的时间

# 文件配置
CHECKPOINT_FILE = 'crawler_checkpoint.json'
PROXY_CACHE_FILE = 'proxy_cache.json'  # 代理缓存文件
OUTPUT_EXCEL = 'XXX.xlsx' # 自定义文件名
OUTPUT_CSV = 'XXX.csv' # 自定义文件名

# 创建session
session = requests.Session()
session.headers.update(HEADERS)

# ==================== 代理管理 ====================

def save_proxy_cache():
    """保存代理缓存"""
    try:
        cache = {
            'proxy_list': PROXY_LIST,
            'last_refresh': last_proxy_refresh,
            'timestamp': datetime.now().isoformat()
        }
        with open(PROXY_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"保存代理缓存失败: {e}", 'WARNING')

def load_proxy_cache():
    """加载代理缓存"""
    global PROXY_LIST, last_proxy_refresh
    
    if os.path.exists(PROXY_CACHE_FILE):
        try:
            with open(PROXY_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            
            cache_time = datetime.fromisoformat(cache.get('timestamp', ''))
            age_minutes = (datetime.now() - cache_time).total_seconds() / 60
            
            # 私密代理有效期2-4小时，如果缓存超过4小时，认为代理已失效
            if age_minutes > 240:  # 4小时
                log(f"代理缓存已过期 ({age_minutes/60:.1f}小时)，不加载", 'WARNING')
                return False
            
            PROXY_LIST = cache.get('proxy_list', [])
            last_proxy_refresh = cache.get('last_refresh', 0)
            
            if PROXY_LIST:
                log(f"加载代理缓存: {len(PROXY_LIST)}个IP (缓存时间: {age_minutes:.1f}分钟前，有效期2-4小时)", 'INFO')
                return True
        except Exception as e:
            log(f"加载代理缓存失败: {e}", 'WARNING')
    
    return False

def fetch_kdl_proxies():
    """从代理API获取代理列表"""
    global proxy_fetch_failed_time
    
    try:
        response = requests.get(KDL_API_URL, timeout=10)
        data = response.json()
        if data.get('code') == 0:
            proxy_list = data.get('data', {}).get('proxy_list', [])
            log(f"成功获取代理: {len(proxy_list)}个", 'SUCCESS')
            proxy_fetch_failed_time = 0  # 重置失败时间
            
            # 保存到缓存
            save_proxy_cache()
            
            return proxy_list
        else:
            error_msg = data.get('msg', '未知错误')
            log(f"代理API返回错误: {error_msg}", 'ERROR')
            
            # 如果是超限错误，记录失败时间
            if '超限' in error_msg or 'limit' in error_msg.lower():
                proxy_fetch_failed_time = time.time()
                log(f"代理提取超限，将在{PROXY_RETRY_DELAY/60:.0f}分钟后重试", 'WARNING')
            
            return []
    except Exception as e:
        log(f"获取代理失败: {e}", 'ERROR')
        return []

def get_proxy():
    """获取代理"""
    global current_proxy_index, PROXY_LIST, last_proxy_refresh, proxy_fetch_failed_time
    
    if not USE_PROXY:
        return None
    
    # 代理模式
    if PROXY_TYPE == 'kdl':
        current_time = time.time()
        
        # 首次运行时加载缓存
        if not PROXY_LIST and last_proxy_refresh == 0:
            load_proxy_cache()
        
        # 检查是否在失败等待期内
        if proxy_fetch_failed_time > 0:
            if (current_time - proxy_fetch_failed_time) < PROXY_RETRY_DELAY:
                # 仍在等待期内，使用现有代理或不使用代理
                if PROXY_LIST:
                    # 继续使用现有代理
                    if PROXY_ROTATION:
                        proxy_ip = PROXY_LIST[current_proxy_index]
                        current_proxy_index = (current_proxy_index + 1) % len(PROXY_LIST)
                    else:
                        proxy_ip = PROXY_LIST[0]
                    
                    proxy_url = f"http://{KDL_USERNAME}:{KDL_PASSWORD}@{proxy_ip}/"
                    return {'http': proxy_url, 'https': proxy_url}
                else:
                    # 没有可用代理，不使用代理
                    return None
            else:
                # 等待期结束，重置失败时间
                proxy_fetch_failed_time = 0
        
        # 首次获取或需要刷新
        if not PROXY_LIST or (current_time - last_proxy_refresh) >= PROXY_REFRESH_INTERVAL:
            log(f"刷新代理IP池（上次刷新: {(current_time - last_proxy_refresh)/60:.1f}分钟前）", 'INFO')
            new_proxies = fetch_kdl_proxies()
            
            if new_proxies:
                PROXY_LIST = new_proxies
                last_proxy_refresh = current_time
                current_proxy_index = 0
            elif not PROXY_LIST:
                # 获取失败且没有现有代理
                log("代理列表为空，本次请求不使用代理", 'WARNING')
                return None
            # 如果获取失败但有现有代理，继续使用现有代理
        
        # 使用代理
        if PROXY_LIST:
            if PROXY_ROTATION:
                proxy_ip = PROXY_LIST[current_proxy_index]
                current_proxy_index = (current_proxy_index + 1) % len(PROXY_LIST)
            else:
                proxy_ip = PROXY_LIST[0]
            
            proxy_url = f"http://{KDL_USERNAME}:{KDL_PASSWORD}@{proxy_ip}/"
            return {'http': proxy_url, 'https': proxy_url}
        else:
            return None
    
    # 手动配置模式
    elif PROXY_TYPE == 'manual':
        if not PROXY_LIST:
            return None
        
        if PROXY_ROTATION:
            proxy = PROXY_LIST[current_proxy_index]
            current_proxy_index = (current_proxy_index + 1) % len(PROXY_LIST)
            return {'http': proxy, 'https': proxy}
        else:
            proxy = PROXY_LIST[0]
            return {'http': proxy, 'https': proxy}
    
    return None

def test_proxy(proxy_dict):
    """测试代理是否可用"""
    try:
        response = requests.get('http://www.baidu.com', proxies=proxy_dict, timeout=10)
        return response.status_code == 200
    except:
        return False

# ==================== 工具函数 ====================

def log(message, level='INFO'):
    """打印日志"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    levels = {'INFO': '[信息]', 'SUCCESS': '[成功]', 'WARNING': '[警告]', 'ERROR': '[错误]'}
    prefix = levels.get(level, '[信息]')
    print(f"[{timestamp}] {prefix} {message}", flush=True)

def get_page(url, retries=MAX_RETRIES, is_detail=False):
    """获取页面内容 - 模拟真实用户行为"""
    global connection_reset_count, server_error_count, PROXY_LIST, current_proxy_index
    
    for i in range(retries):
        try:
            # 更新Referer，模拟真实浏览路径
            if is_detail:
                session.headers['Referer'] = LIST_URL.format(1)
            
            # 获取代理
            proxies = get_proxy()
            current_proxy_ip = None
            if proxies and i == 0:  # 首次请求显示代理信息
                proxy_ip = proxies.get('http', 'None').split('@')[-1] if '@' in proxies.get('http', '') else proxies.get('http', 'None')
                current_proxy_ip = proxy_ip.rstrip('/')
                log(f"使用代理: {proxy_ip}", 'INFO')
            
            # 添加随机延时，模拟人类思考
            if i > 0:
                wait_time = random.uniform(*DELAY_ERROR)
                log(f"等待 {wait_time:.0f} 秒后重试...", 'WARNING')
                time.sleep(wait_time)
            
            response = session.get(url, timeout=60, proxies=proxies)
            response.encoding = 'utf-8'
            
            if response.status_code == 200:
                # 成功请求，逐步降低错误计数器
                if connection_reset_count > 0:
                    connection_reset_count = max(0, connection_reset_count - 1)
                if server_error_count > 0:
                    server_error_count = max(0, server_error_count - 1)
                # 适度延时，模拟阅读
                time.sleep(random.uniform(1.0, 2.0))
                return response.text
            elif response.status_code == 429:  # Too Many Requests
                log(f"请求过于频繁(429)，等待更长时间", 'WARNING')
                time.sleep(random.uniform(120, 180))  # 等待2-3分钟
            elif response.status_code == 500:  # Internal Server Error
                server_error_count += 1
                log(f"服务器负载警告(500) - 第 {server_error_count} 次", 'WARNING')
                
                # 根据500错误次数动态调整等待时间
                if server_error_count >= 3:
                    wait_time = random.uniform(60, 90)  # 多次500错误，等待更久
                    log(f"服务器负载较高，等待 {wait_time:.0f} 秒以减轻压力", 'WARNING')
                else:
                    wait_time = random.uniform(20, 35)
                    log(f"等待 {wait_time:.0f} 秒后重试", 'WARNING')
                time.sleep(wait_time)
            elif response.status_code == 503:  # Service Unavailable
                log(f"服务不可用(503)，服务器过载", 'WARNING')
                time.sleep(random.uniform(60, 90))  # 等待1-1.5分钟
            else:
                log(f"状态码: {response.status_code}", 'WARNING')
                time.sleep(random.uniform(15, 25))
                
        except requests.exceptions.Timeout:
            log(f"请求超时，重试 {i+1}/{retries}", 'WARNING')
            time.sleep(random.uniform(30, 50))
            
        except requests.exceptions.ConnectionError as e:
            error_str = str(e)
            
            # 检测连接重置错误 (10054)
            if '10054' in error_str or 'Connection aborted' in error_str or 'ConnectionResetError' in error_str:
                connection_reset_count += 1
                log(f"!!! 连接被服务器强制关闭 (10054) - 第 {connection_reset_count} 次 !!!", 'ERROR')
                log(f"这表明触发了服务器安全策略，需要大幅降低请求频率", 'ERROR')
                
                # 检查是否超过最大重置次数
                if connection_reset_count >= MAX_CONNECTION_RESETS:
                    log(f"连接重置次数达到上限({MAX_CONNECTION_RESETS})，停止爬取", 'ERROR')
                    log(f"建议：1) 增加延时配置 2) 更换IP 3) 明天再试", 'ERROR')
                    raise ConnectionAbortedError(f"连接被重置{connection_reset_count}次，触发安全策略，停止爬取")
                
                # 长时间等待
                wait_time = random.uniform(*DELAY_CONNECTION_RESET)
                log(f"等待 {wait_time/60:.1f} 分钟后继续...", 'WARNING')
                time.sleep(wait_time)
            # 检测代理连接错误
            elif 'Max retries exceeded' in error_str and current_proxy_ip and PROXY_LIST:
                log(f"代理连接失败: {current_proxy_ip}，从列表中移除", 'WARNING')
                # 从代理列表中移除失效的代理
                try:
                    # 提取IP:端口
                    failed_proxy = current_proxy_ip.split('/')[-1] if '/' in current_proxy_ip else current_proxy_ip
                    if failed_proxy in PROXY_LIST:
                        PROXY_LIST.remove(failed_proxy)
                        log(f"已移除失效代理，剩余 {len(PROXY_LIST)} 个", 'INFO')
                        # 更新缓存
                        save_proxy_cache()
                        # 重置索引
                        if current_proxy_index >= len(PROXY_LIST) and PROXY_LIST:
                            current_proxy_index = 0
                        
                        # 尝试获取新代理补充
                        if PROXY_TYPE == 'kdl' and proxy_fetch_failed_time == 0:
                            log(f"尝试获取新代理补充...", 'INFO')
                            new_proxies = fetch_kdl_proxies()
                            if new_proxies:
                                # 只添加新获取的代理，不替换现有的
                                for new_proxy in new_proxies:
                                    if new_proxy not in PROXY_LIST:
                                        PROXY_LIST.append(new_proxy)
                                log(f"成功补充代理，当前共 {len(PROXY_LIST)} 个", 'SUCCESS')
                                save_proxy_cache()
                            else:
                                log(f"获取新代理失败，继续使用现有代理", 'WARNING')
                        elif proxy_fetch_failed_time > 0:
                            log(f"代理API仍在超限期，不强行提取", 'INFO')
                        
                except Exception as remove_error:
                    log(f"移除代理失败: {remove_error}", 'WARNING')
                
                # 如果还有其他代理，继续重试
                if PROXY_LIST:
                    log(f"切换到其他代理重试", 'INFO')
                    time.sleep(random.uniform(5, 10))
                else:
                    log(f"所有代理都已失效，不使用代理继续", 'WARNING')
                    time.sleep(random.uniform(30, 50))
            else:
                log(f"连接错误: {error_str[:80]}", 'ERROR')
                time.sleep(random.uniform(60, 90))
                
        except Exception as e:
            log(f"请求异常: {str(e)[:80]}", 'ERROR')
            time.sleep(random.uniform(30, 50))
    
    return None

def save_checkpoint(data, current_page, current_case_index=0):
    """保存检查点 - 包含精确位置"""
    checkpoint = {
        'data': data,
        'current_page': current_page,
        'current_case_index': current_case_index,  # 新增：当前页内的案例索引
        'timestamp': datetime.now().isoformat(),
        'count': len(data)
    }
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=2)

def load_checkpoint():
    """加载检查点"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
            
            # 方案2：加载时去重
            data = checkpoint.get('data', [])
            if data:
                # 使用案例编号去重
                seen = set()
                unique_data = []
                for item in data:
                    case_no = item.get('案例编号', '')
                    if case_no and case_no not in seen:
                        seen.add(case_no)
                        unique_data.append(item)
                
                if len(unique_data) < len(data):
                    log(f"检查点去重: {len(data)} -> {len(unique_data)} 条", 'WARNING')
                    checkpoint['data'] = unique_data
                    checkpoint['count'] = len(unique_data)
            
            return checkpoint
        except Exception as e:
            log(f"加载检查点失败: {e}", 'ERROR')
            return None
    else:
        log("未找到检查点文件，从头开始", 'INFO')
        return None

# ==================== 解析函数 ====================

def parse_list_page(html):
    """解析列表页"""
    soup = BeautifulSoup(html, 'html.parser')
    cases = []
    
    table = soup.find('table')
    if table:
        rows = table.find_all('tr')[1:]  # 跳过表头
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 4:
                case_no = cols[0].text.strip()
                case_name_link = cols[1].find('a')
                case_name = case_name_link.text.strip() if case_name_link else cols[1].text.strip()
                case_href = case_name_link.get('href') if case_name_link else None
                author = cols[2].text.strip()
                publish_date = cols[3].text.strip()
                
                cases.append({
                    '案例编号': case_no,
                    '案例名称': case_name,
                    '作者': author,
                    '发布日期': publish_date,
                    'detail_href': case_href,
                })
    
    return cases

def parse_detail_page(html, basic_info):
    """解析详情页"""
    soup = BeautifulSoup(html, 'html.parser')
    detail = basic_info.copy()
    detail.pop('detail_href', None)
    
    table = soup.find('table')
    if table:
        rows = table.find_all('tr')
        for row in rows:
            th = row.find('th')
            td = row.find('td')
            if th and td:
                key = th.text.strip().replace('：', '').replace(':', '')
                
                # 检查是否有链接
                link = td.find('a')
                if link and link.get('href'):
                    href = link.get('href')
                    value = href if href.startswith('http') else urljoin(BASE_URL, href)
                else:
                    value = td.get_text(separator=' ', strip=True)
                
                if key not in detail:
                    detail[key] = value
    
    return detail

def get_total_pages():
    """获取总页数"""
    html = get_page(LIST_URL.format(1))
    if html:
        match = re.search(r'查询到有(\d+)个案例', html)
        if match:
            total_cases = int(match.group(1))
            total_pages = (total_cases + 17) // 18
            log(f"检测到 {total_cases} 个案例，共 {total_pages} 页")
            return total_pages
    return 600

# ==================== 主爬取函数 ====================

def scrape_cases():
    """爬取案例"""
    global connection_reset_count, server_error_count, start_time
    all_cases = []
    start_page = 1
    start_case_index = 0  # 新增：起始案例索引
    
    # 记录开始时间
    start_time = time.time()
    
    # 加载检查点
    checkpoint = load_checkpoint()
    if checkpoint:
        all_cases = checkpoint['data']
        start_page = checkpoint['current_page']
        start_case_index = checkpoint.get('current_case_index', 0)  # 获取案例索引
        log(f"从检查点恢复: 第{start_page}页第{start_case_index}个案例，已有{len(all_cases)}条数据")
    
    # 获取总页数
    total_pages = get_total_pages()
    
    log("=" * 60)
    log(f"开始爬取: 第{start_page}页 到 第{total_pages}页")
    log(f"延时策略: 请求间隔{DELAY_REQUEST[0]}-{DELAY_REQUEST[1]}秒")
    log(f"每{BATCH_SIZE}条休息{DELAY_BATCH[0]/60:.1f}-{DELAY_BATCH[1]/60:.1f}分钟")
    log(f"每{CASES_PER_SESSION}条长休息3-4分钟")
    if ENABLE_TIME_LIMIT:
        log(f"分时段模式: 单次运行{MAX_RUN_TIME/3600:.1f}小时后自动停止")
        log(f"建议休息: {SUGGEST_REST_TIME/3600:.1f}小时后继续")
    log("超保守模式: 最大程度保护服务器")
    log("三重去重保护: 精确位置 + 加载去重 + 保存去重")
    log("=" * 60)
    
    try:
        for page in range(start_page, total_pages + 1):
            # 检查运行时间
            if ENABLE_TIME_LIMIT and start_time:
                elapsed_time = time.time() - start_time
                if elapsed_time >= MAX_RUN_TIME:
                    log("\n" + "=" * 60, 'WARNING')
                    log(f"已运行 {elapsed_time/3600:.1f} 小时，达到单次运行时间限制", 'WARNING')
                    log(f"自动停止以保护服务器，避免长时间连续爬取", 'WARNING')
                    save_checkpoint(all_cases, page, 0)
                    log(f"已保存 {len(all_cases)} 条数据到检查点", 'SUCCESS')
                    log("=" * 60, 'WARNING')
                    log(f"\n建议休息 {SUGGEST_REST_TIME/3600:.1f} 小时后继续运行：", 'INFO')
                    log(f"python crawler_optimized.py", 'INFO')
                    log("\n分时段爬取的好处：", 'INFO')
                    log("1. 模拟真实用户行为（没人连续访问十几小时）", 'INFO')
                    log("2. 避免长时间连接被服务器标记", 'INFO')
                    log("3. 给服务器充分休息时间", 'INFO')
                    log("4. 降低触发安全策略的风险", 'INFO')
                    return all_cases
                elif elapsed_time >= MIN_RUN_TIME and elapsed_time % 1800 < 60:  # 每30分钟提示一次
                    remaining = MAX_RUN_TIME - elapsed_time
                    log(f"  [时间提示] 已运行 {elapsed_time/3600:.1f}h，还可运行 {remaining/3600:.1f}h", 'INFO')
            
            log(f"【第 {page}/{total_pages} 页】 进度: {page/total_pages*100:.1f}% | 连接重置: {connection_reset_count}次 | 500错误: {server_error_count}次")
            
            # 获取列表页
            url = LIST_URL.format(page)
            html = get_page(url)
            
            if not html:
                log(f"第 {page} 页获取失败，保存检查点", 'ERROR')
                save_checkpoint(all_cases, page)
                
                # 如果连续失败，增加等待时间
                log(f"页面获取失败，等待后继续...", 'WARNING')
                time.sleep(random.uniform(90, 150))
                continue
            
            cases = parse_list_page(html)
            log(f"  找到 {len(cases)} 个案例")
            
            # 获取详情
            for idx, case in enumerate(cases):
                # 如果是恢复的页面，跳过已处理的案例
                if page == start_page and idx < start_case_index:
                    log(f"  [{idx+1}/{len(cases)}] 跳过已处理的案例", 'INFO')
                    continue
                
                case_name_short = case['案例名称'][:20] + '...' if len(case['案例名称']) > 20 else case['案例名称']
                log(f"  [{idx+1}/{len(cases)}] {case['案例编号']} - {case_name_short}")
                
                # 方案3：添加前检查是否已存在（实时去重）
                case_no = case.get('案例编号', '')
                if case_no and any(c.get('案例编号') == case_no for c in all_cases):
                    log(f"    案例已存在，跳过", 'WARNING')
                    continue
                
                if case.get('detail_href'):
                    detail_url = urljoin(BASE_URL, case['detail_href'])
                    detail_html = get_page(detail_url, is_detail=True)
                    
                    if detail_html:
                        detail = parse_detail_page(detail_html, case)
                        all_cases.append(detail)
                    else:
                        log(f"    详情获取失败，跳过此案例", 'WARNING')
                        case.pop('detail_href', None)
                        all_cases.append(case)
                else:
                    case.pop('detail_href', None)
                    all_cases.append(case)
                
                # 模拟真实用户的阅读和思考时间
                read_time = random.uniform(*DELAY_REQUEST)
                time.sleep(read_time)
                
                # 分批保存 - 方案1：保存精确位置
                if len(all_cases) % BATCH_SIZE == 0:
                    save_checkpoint(all_cases, page, idx + 1)  # 保存下一个案例的索引
                    log(f"  [检查点] 已保存 {len(all_cases)} 条数据 (第{page}页第{idx+1}个)", 'SUCCESS')
                    rest_time = random.uniform(*DELAY_BATCH)
                    log(f"  [休息] 等待 {rest_time:.0f} 秒...", 'INFO')
                    time.sleep(rest_time)
                
                # 每60条长时间休息，模拟用户会话结束
                if len(all_cases) % CASES_PER_SESSION == 0:
                    long_rest = random.uniform(180, 240)  # 3-4分钟
                    log(f"  [长休息] 已爬取 {len(all_cases)} 条，休息 {long_rest/60:.1f} 分钟，让服务器充分休息...", 'INFO')
                    time.sleep(long_rest)
                    # 长休息后重置500错误计数
                    server_error_count = 0
            
            # 每页保存 - 方案1：保存到下一页的起始位置
            save_checkpoint(all_cases, page + 1, 0)
            
            # 重置起始索引（只在第一页有效）
            if page == start_page:
                start_case_index = 0
            
            # 页面延时 - 模拟用户翻页思考
            if page < total_pages:
                page_delay = random.uniform(*DELAY_PAGE)
                log(f"  翻页延时 {page_delay:.1f} 秒...", 'INFO')
                time.sleep(page_delay)
                
    except KeyboardInterrupt:
        log("\n用户中断，立即保存当前进度", 'WARNING')
        # 方案1+3：保存精确位置和当前数据
        current_idx = idx if 'idx' in locals() else 0
        save_checkpoint(all_cases, page if 'page' in locals() else start_page, current_idx)
        log(f"已保存 {len(all_cases)} 条数据 (第{page if 'page' in locals() else start_page}页第{current_idx}个)", 'SUCCESS')
        return all_cases
    except ConnectionAbortedError as e:
        log(f"\n!!! 触发安全策略，停止爬取 !!!", 'ERROR')
        log(f"原因: {e}", 'ERROR')
        current_idx = idx if 'idx' in locals() else 0
        save_checkpoint(all_cases, page if 'page' in locals() else start_page, current_idx)
        log(f"已保存 {len(all_cases)} 条数据到检查点", 'SUCCESS')
        log("\n建议优化措施:", 'WARNING')
        log("1. 将 DELAY_REQUEST 增加到 (6, 12) 秒", 'WARNING')
        log("2. 将 DELAY_PAGE 增加到 (20, 40) 秒", 'WARNING')
        log("3. 将 BATCH_SIZE 减少到 10", 'WARNING')
        log("4. 将 CASES_PER_SESSION 减少到 30", 'WARNING')
        log("5. 考虑更换网络环境或使用代理", 'WARNING')
        log("6. 在非高峰时段（凌晨2-6点）运行", 'WARNING')
        return all_cases
    except Exception as e:
        log(f"发生错误: {e}", 'ERROR')
        current_idx = idx if 'idx' in locals() else 0
        save_checkpoint(all_cases, page if 'page' in locals() else start_page, current_idx)
        raise
    
    return all_cases

# ==================== 保存结果 ====================

def save_results(data):
    """保存结果"""
    if not data:
        log("没有数据可保存", 'ERROR')
        return
    
    # 方案2：保存前最终去重
    seen = set()
    unique_data = []
    for item in data:
        case_no = item.get('案例编号', '')
        if case_no and case_no not in seen:
            seen.add(case_no)
            unique_data.append(item)
    
    if len(unique_data) < len(data):
        log(f"保存前去重: {len(data)} -> {len(unique_data)} 条", 'WARNING')
        data = unique_data
    
    df = pd.DataFrame(data)
    
    # 列顺序（根据自己情况设置）
    priority_cols = [
        'XXX'
    ]
    
    existing_cols = [c for c in priority_cols if c in df.columns]
    other_cols = [c for c in df.columns if c not in priority_cols]
    df = df[existing_cols + other_cols]
    
    # 保存Excel
    try:
        df.to_excel(OUTPUT_EXCEL, index=False, engine='openpyxl')
        log(f"Excel已保存: {OUTPUT_EXCEL}", 'SUCCESS')
    except KeyboardInterrupt:
        raise  # 重新抛出键盘中断
    except Exception as e:
        log(f"Excel保存失败: {e}", 'ERROR')
        # 尝试使用xlsxwriter引擎
        try:
            df.to_excel(OUTPUT_EXCEL, index=False, engine='xlsxwriter')
            log(f"Excel已保存(xlsxwriter): {OUTPUT_EXCEL}", 'SUCCESS')
        except:
            log(f"Excel保存失败，仅保存CSV", 'WARNING')
    
    # 保存CSV
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    log(f"CSV已保存: {OUTPUT_CSV}", 'SUCCESS')
    
    log(f"\n总计: {len(data)} 条案例, {len(df.columns)} 个字段", 'SUCCESS')

def cleanup():
    """清理检查点"""
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        log("检查点文件已清理")

# ==================== 主函数 ====================

def main():
    """主入口"""
    log("=" * 60)
    log("XXX - 案例爬虫（超保守版）")
    log("作者: XXX")
    log("=" * 60)
    log("安全策略监控: 已启用")
    log(f"最大连接重置次数: {MAX_CONNECTION_RESETS}")
    log(f"当前延时策略: 请求{DELAY_REQUEST[0]}-{DELAY_REQUEST[1]}秒")
    log(f"分时段模式: {'启用' if ENABLE_TIME_LIMIT else '未启用'}")
    if ENABLE_TIME_LIMIT:
        log(f"单次运行时间: {MAX_RUN_TIME/3600:.1f}小时")
        log(f"建议休息时间: {SUGGEST_REST_TIME/3600:.1f}小时")
    log(f"代理模式: {'启用' if USE_PROXY else '未启用'}")
    if USE_PROXY:
        if PROXY_TYPE == 'kdl':
            log(f"代理类型: XXX")
            log(f"代理轮换: {'启用' if PROXY_ROTATION else '未启用'}")
            log(f"刷新间隔: {PROXY_REFRESH_INTERVAL/60:.0f}分钟 (每次获取3个IP)")
            log(f"速率限制: 150分钟内最多3次刷新=9个IP (限制20个)")
            log(f"超限等待: {PROXY_RETRY_DELAY/60:.0f}分钟")
        elif PROXY_TYPE == 'manual' and PROXY_LIST:
            log(f"代理类型: 手动配置")
            log(f"代理数量: {len(PROXY_LIST)}个")
            log(f"代理轮换: {'启用' if PROXY_ROTATION else '未启用'}")
    log("设计理念: 优先保护服务器，避免造成负担")
    log("=" * 60)
    log("")
    
    try:
        # 爬取数据
        data = scrape_cases()
        
        # 保存结果
        if data:
            save_results(data)
            # 只有正常完成全部爬取才清理检查点
            if connection_reset_count < MAX_CONNECTION_RESETS:
                # 检查是否真的完成了
                checkpoint = load_checkpoint()
                if checkpoint and checkpoint.get('current_page', 1) >= 589:
                    cleanup()
                    log("\n全部爬取完成！", 'SUCCESS')
                else:
                    log("\n本次运行完成（保留检查点以便继续）", 'SUCCESS')
            else:
                log("\n爬取完成（保留检查点）", 'SUCCESS')
        else:
            log("未获取到数据", 'ERROR')
    except ConnectionAbortedError:
        log("\n程序因触发安全策略而停止", 'ERROR')
        log("请根据上述建议优化配置后重新运行", 'WARNING')
    except KeyboardInterrupt:
        log("\n用户手动中断（检查点已保存）", 'WARNING')
    except Exception as e:
        log(f"\n程序异常退出: {e}", 'ERROR')
        raise

if __name__ == '__main__':
    main()
