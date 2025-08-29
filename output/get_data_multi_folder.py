# -*- coding: utf-8 -*-
"""
功能：
- 递归扫描 ROOT_DIR 下的所有子目录，凡是目录内含 data_pages/ 的，都当作一个 crawler 目录处理
- 在每个 crawler 的 data_pages/ 中选择“最新”的 .deflate（按 mtime）
- 按行解包 JSONL，每行写成独立 JSON 文件：OUTPUT_ROOT/<crawler_name>/1.json, 2.json, ...
- 自动尝试 raw DEFLATE 与标准 zlib 两种解压方式，提高兼容性
- 日志打印每个目录的耗时与吞吐率，最后汇总统计

用法：
  直接运行，按需修改 ROOT_DIR 与 OUTPUT_ROOT。
"""

import os
import zlib
import json
import base64
import time
from typing import Iterator, Tuple, Optional

# =============== 可调参数 ===============
# 放各个 crawler 目录的上级目录（会递归扫描）
ROOT_DIR    = r'./server-cracking-zhaorong/new2'

# 输出根目录（每个 crawler 会在此目录下创建同名子目录并写 1.json、2.json…）
OUTPUT_ROOT = r'cracking-to-json-zhaorong'
# ======================================

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
    """挑选最新的 .deflate"""
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
    # 支持 \n/\r\n
    lines = decompressed_text.splitlines()

    for idx, line in enumerate(lines, start=1):
        if not line.strip():
            # 空行直接跳过，但计数保留
            yield None, idx
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and isinstance(obj.get('content'), str):
                # content 尝试 base64 → utf-8
                try:
                    obj['content'] = base64.b64decode(obj['content']).decode('utf-8')
                except Exception:
                    pass
            yield obj, idx
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            dprint(f"[WARN] {os.path.basename(file_path)} 第 {idx} 行 JSON/Unicode 解析失败: {e}")
            yield None, idx

def save_json_to_file(json_object: dict, index: int, output_folder: str):
    """用行号作为文件名：<index>.json（存在将覆盖）"""
    ensure_dir(output_folder)
    out_path = os.path.join(output_folder, f"{index}.json")
    with open(out_path, 'w', encoding='utf-8') as w:
        json.dump(json_object, w, indent=2, ensure_ascii=False)

def find_crawler_dirs(root: str):
    """
    递归遍历 root，凡是目录内直接包含 'data_pages' 子目录的，都当作一个 crawler 目录。
    返回去重后的绝对路径列表（稳定排序）。
    """
    if not os.path.isdir(root):
        raise FileNotFoundError(f"目录不存在: {root}")
    crawler_dirs = []
    for curdir, dirnames, _ in os.walk(root):
        if 'data_pages' in dirnames:
            crawler_dirs.append(os.path.abspath(curdir))
            # 为避免深入 data_pages 提高效率，可不再进入该子目录
            # 但仍需遍历其他子目录
    crawler_dirs = sorted(set(crawler_dirs))
    return crawler_dirs

def process_one_crawler_dir(crawler_dir: str) -> Tuple[int, int, float]:
    """
    处理一个 crawler：
      - 选最新 .deflate
      - 按行写出 1.json、2.json…
    返回：有效条数、总行数、耗时秒
    """
    t0 = time.perf_counter()
    crawler_name = os.path.basename(crawler_dir)
    data_pages_dir = os.path.join(crawler_dir, 'data_pages')

    try:
        file_path = pick_latest_deflate(data_pages_dir)
    except FileNotFoundError as e:
        dprint(f"[INFO] {crawler_name}: {e}，跳过。")
        return (0, 0, 0.0)

    output_folder = os.path.join(OUTPUT_ROOT, crawler_name)
    dprint(f"[INFO] {crawler_name}: 使用 .deflate => {file_path}")
    dprint(f"[INFO] {crawler_name}: 输出目录 => {output_folder}")

    total_ok = total_lines = 0
    for json_object, index in read_deflate_file_line_by_line(file_path):
        total_lines += 1
        if json_object is not None:
            save_json_to_file(json_object, index, output_folder)
            total_ok += 1

    dt = time.perf_counter() - t0
    rate = (total_ok / dt) if dt > 0 else 0.0
    dprint(f"[DONE] {crawler_name}: 有效 {total_ok} / 总行 {total_lines}  用时 {dt:.2f}s  速度 {rate:.1f} rec/s")
    return (total_ok, total_lines, dt)

def main():
    ensure_dir(OUTPUT_ROOT)
    dprint(f"[INFO] 扫描根目录: {os.path.abspath(ROOT_DIR)}")
    dprint(f"[INFO] 输出根目录: {os.path.abspath(OUTPUT_ROOT)}")

    try:
        crawler_dirs = find_crawler_dirs(ROOT_DIR)
    except FileNotFoundError as e:
        dprint(f"[ERROR] {e}")
        return

    dprint(f"[SCAN] 共发现 {len(crawler_dirs)} 个包含 data_pages/ 的目录")
    grand_ok = grand_lines = 0
    grand_time = 0.0

    for i, cdir in enumerate(crawler_dirs, 1):
        rel = os.path.relpath(cdir, start=ROOT_DIR)
        dprint(f"[START] ({i}/{len(crawler_dirs)}) 处理目录：{rel}")
        ok, lines, dt = process_one_crawler_dir(cdir)
        grand_ok += ok
        grand_lines += lines
        grand_time += dt

    if not crawler_dirs:
        dprint("[WARN] 未发现可处理目录。")
    else:
        rate = (grand_ok / grand_time) if grand_time > 0 else 0.0
        dprint(f"[SUMMARY] 全部完成：有效记录 {grand_ok} / 总行 {grand_lines}  总耗时 {grand_time:.2f}s  平均 {rate:.1f} rec/s")
        dprint(f"[SUMMARY] 输出根目录：{os.path.abspath(OUTPUT_ROOT)}")

if __name__ == "__main__":
    main()
