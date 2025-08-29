# -*- coding: utf-8 -*-
"""
批量把指定目录结构下最新的 .deflate（JSONL）逐行解包为单条 JSON 文件：
- 仅匹配目录名形如：
    <PREFIX><num>
    <PREFIX><num>_<timestamp>
  其中 <PREFIX> 由你指定，如 'cracking_forum_urls_level_2_batch_1_to_5_'
- 只排除出现在“前缀之后”的 'to'（前缀本身含 'to' 不受影响）
- 仅处理批次号在 [BATCH_START, BATCH_END] 的目录
- 每个匹配目录里，进入 data_pages/，选取最新 .deflate，按行解包
- 输出到 OUTPUT_ROOT/<目录名>/1.json, 2.json, ...
"""

import os
import re
import zlib
import json
import base64
from typing import Iterator, Tuple, Optional

# =============== 可调参数 ===============
# 放各个 batch 目录的上级目录（按你的实际路径改：如 './server-cracking' 或 './server-test'）
ROOT_DIR    = './server-cracking'

# 目录前缀（必须精确到最后的下划线；示例：'cracking_forum_urls_level_2_batch_1_to_5_'）
PREFIX      = 'all_seeds_need_processing_level_1_seed_pending_10_to_13_thread_links_'

# 处理的批次范围（闭区间）
BATCH_START = 1
BATCH_END   = 6

# 输出根目录（每个 crawler 会在此目录下创建同名子目录并写 1.json、2.json…）
OUTPUT_ROOT = 'cracking-to-json'
# ======================================

# 允许的目录名形式：
#   <PREFIX><num>
#   <PREFIX><num>_<digits>   # <digits> 为时间戳纯数字
DIR_REGEX = re.compile(
    rf'^{re.escape(PREFIX)}(?P<num>\d+)(?:_(?P<ts>\d+))?$',
    re.IGNORECASE
)

# -------- 小工具：立即刷新的打印 --------
def dprint(*args):
    print(*args, flush=True)

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def list_deflate_files(data_pages_dir: str) -> list:
    """列出 data_pages 下所有 .deflate，按 mtime 降序（最新在前）"""
    if not os.path.isdir(data_pages_dir):
        return []
    files = [
        os.path.join(data_pages_dir, f)
        for f in os.listdir(data_pages_dir)
        if f.lower().endswith('.deflate')
    ]
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files

def pick_latest_deflate(data_pages_dir: str) -> str:
    """挑选最新的 .deflate（与单个脚本一致）"""
    candidates = list_deflate_files(data_pages_dir)
    if not candidates:
        raise FileNotFoundError(f"未在 {data_pages_dir} 找到 .deflate 文件")
    return candidates[0]

def _zlib_decompress_best_effort(blob: bytes) -> bytes:
    """先尝试标准 zlib，再回退 raw DEFLATE，提高兼容性"""
    try:
        return zlib.decompress(blob)
    except Exception:
        try:
            d = zlib.decompressobj(-zlib.MAX_WBITS)
            out = d.decompress(blob)
            out += d.flush()
            return out
        except Exception as e:
            raise zlib.error(f"DEFLATE 解压失败: {e}")

def read_deflate_file_line_by_line(file_path: str) -> Iterator[Tuple[Optional[dict], int]]:
    """按行读取 JSONL；含 content 字段则尝试 base64 解码（失败保留原值）"""
    with open(file_path, 'rb') as file:
        compressed_data = file.read()

    try:
        decompressed_data = _zlib_decompress_best_effort(compressed_data)
    except zlib.error as e:
        dprint(f"[ERROR] {os.path.basename(file_path)} 解压失败: {e}")
        return

    decompressed_text = decompressed_data.decode('utf-8', errors='replace')
    lines = decompressed_text.splitlines()

    for idx, line in enumerate(lines, start=1):
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and isinstance(obj.get('content'), str):
                try:
                    obj['content'] = base64.b64decode(obj['content']).decode('utf-8')
                except Exception:
                    pass
            yield obj, idx
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            dprint(f"[WARN] {os.path.basename(file_path)} 第 {idx} 行 JSON/Unicode 解析失败: {e}")
            yield None, idx

def save_json_to_file(json_object: dict, index: int, output_folder: str):
    """与单个脚本一致：用行号作为文件名：<index>.json"""
    ensure_dir(output_folder)
    out_path = os.path.join(output_folder, f"{index}.json")
    with open(out_path, 'w', encoding='utf-8') as w:
        json.dump(json_object, w, indent=4, ensure_ascii=False)

def iter_matching_crawler_dirs(root: str):
    """
    返回满足条件的子目录：
      1) 目录名以 PREFIX 开头；
      2) 仅在“前缀之后”的部分排除含 'to' 的目录（前缀本身含 'to' 不影响）；
      3) 目录名需与 DIR_REGEX 匹配（PREFIX + 数字 + 可选 _时间戳）；
      4) 批次号在 [BATCH_START, BATCH_END]。
    """
    if not os.path.isdir(root):
        raise FileNotFoundError(f"目录不存在: {root}")

    names = [n for n in os.listdir(root) if os.path.isdir(os.path.join(root, n))]
    dprint(f"[SCAN] ROOT_DIR={os.path.abspath(root)}  下有 {len(names)} 个子目录")
    hits = 0

    for name in names:
        full = os.path.join(root, name)

        # 必须以给定前缀开头
        if not name.startswith(PREFIX):
            continue

        # 只在“前缀之后”的 suffix 中排除 'to'
        suffix = name[len(PREFIX):]
        if 'to' in suffix.lower():
            dprint(f"[SKIP] 后缀包含 'to' → {name}")
            continue

        # 用严格正则再确认形态
        m = DIR_REGEX.match(name)
        if not m:
            continue

        num = int(m.group('num'))
        if BATCH_START <= num <= BATCH_END:
            hits += 1
            dprint(f"[HIT] {name}  (batch={num})")
            yield full

    if hits == 0:
        dprint("[WARN] 未发现满足条件的目录，请检查 PREFIX/BATCH_START/BATCH_END 是否正确。")

def process_one_crawler_dir(crawler_dir: str) -> Tuple[int, int]:
    """处理一个 crawler：挑最新 .deflate，按行写出 1.json、2.json…"""
    crawler_name = os.path.basename(crawler_dir)
    data_pages_dir = os.path.join(crawler_dir, 'data_pages')

    try:
        file_path = pick_latest_deflate(data_pages_dir)
    except FileNotFoundError as e:
        dprint(f"[INFO] {crawler_name}: {e}，跳过。")
        return (0, 0)

    output_folder = os.path.join(OUTPUT_ROOT, crawler_name)
    dprint(f"[INFO] {crawler_name}: 使用 .deflate => {file_path}")
    dprint(f"[INFO] {crawler_name}: 输出目录 => {output_folder}")

    total_ok = total_lines = 0
    for json_object, index in read_deflate_file_line_by_line(file_path):
        total_lines += 1
        if json_object is not None:
            save_json_to_file(json_object, index, output_folder)
            total_ok += 1

    dprint(f"[DONE] {crawler_name}: 有效 {total_ok} / 总行 {total_lines}")
    return (total_ok, total_lines)

def main():
    ensure_dir(OUTPUT_ROOT)
    grand_ok = grand_lines = 0
    found_any = False

    dprint(f"[INFO] 扫描根目录: {os.path.abspath(ROOT_DIR)}")
    dprint(f"[INFO] 目录前缀: {PREFIX}")
    dprint(f"[INFO] 批次范围: [{BATCH_START}, {BATCH_END}]")
    dprint(f"[INFO] 输出根目录: {os.path.abspath(OUTPUT_ROOT)}")

    for cdir in iter_matching_crawler_dirs(ROOT_DIR):
        found_any = True
        ok, lines = process_one_crawler_dir(cdir)
        grand_ok += ok
        grand_lines += lines

    if not found_any:
        dprint(f"[WARN] 未发现满足条件的目录。")
    else:
        dprint(f"[SUMMARY] 全部完成：有效记录 {grand_ok} / 总行 {grand_lines}")
        dprint(f"[SUMMARY] 输出根目录：{os.path.abspath(OUTPUT_ROOT)}")

if __name__ == "__main__":
    main()
