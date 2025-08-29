# -*- coding: utf-8 -*-
"""
extract_pages_to_json.py

功能要点：
- 逐个读取本地 JSON（形如 {"url": "...", "content": "<html>..." }）
- 只按“页面”为单位解析，不跨页聚合
- 提取 XenForo 线程页的元信息与帖子（含楼中楼）
- 所有输出 JSON 放同一个输出目录里累计
- 命名：tid_<线程ID>__p<页码>__<标题slug>.json；若重名 → 自动追加 __dupNNN（绝不覆盖）
- **新增**：按“目录”为粒度打印开始/结束日志与耗时统计，方便你看每个文件夹处理时间

依赖：
  pip install beautifulsoup4

用法：
  python extract_pages_to_json.py
"""

import os
import re
import io
import json
import html
import base64
import hashlib
import logging
import unicodedata
import gzip
import zlib
import time
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlsplit, parse_qs
from bs4 import BeautifulSoup, NavigableString, Tag
from datetime import datetime, timezone

# ============【配置区：改这里】============
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))

# 输入 JSON 根目录（支持递归）
JSON_DIR    = os.path.normpath(os.path.join(SCRIPT_DIR, "../cracking-to-json"))
RECURSIVE   = True

# 输出目录（所有页面 JSON 都写到这里，累计存放）
OUTPUT_DIR  = os.path.normpath(os.path.join(SCRIPT_DIR, "./cracking_pages_json"))
# =========================================

ENCODING       = "utf-8"
MAX_NAME_LEN   = 90
BASE_ORIGIN    = "https://cracking.org"

# 仅处理 thread 路径
THREAD_PATH_FILTER = re.compile(r"/threads?/", re.I)

# 楼中楼容器
REPLY_CONTAINER_CLASSES = {
    "js-tprReplyMessageContainer",
    "js-messageReplies",
    "message-replies",
    "message-response",
    "message-responses",
}

# ---------- 日志 ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("xenforo-page-extract")


# =============== 工具函数 ===============
def ensure_dir(d: str):
    if not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def sanitize(name: Optional[str], max_len: int = MAX_NAME_LEN) -> str:
    RESERVED = {"CON","PRN","AUX","NUL",
                "COM1","COM2","COM3","COM4","COM5","COM6","COM7","COM8","COM9",
                "LPT1","LPT2","LPT3","LPT4","LPT5","LPT6","LPT7","LPT8","LPT9"}
    if not name:
        return "page"
    s = unicodedata.normalize("NFKD", str(name))
    s = re.sub(r"[\x00-\x1F\x7F\u200B-\u200D\uFEFF]", "", s)
    s = re.sub(r'[<>:"/\\|?*]+', "_", s)
    s = re.sub(r"\s+", "_", s).strip(" ._")
    s = re.sub(r"_+", "_", s)
    if not s:
        s = "page"
    if s.upper() in RESERVED:
        s = f"_{s}"
    if len(s) > max_len:
        s = s[:max_len]
    return s

def make_unique_json_path(out_dir: str, base_filename: str) -> str:
    """
    保证不覆盖：若文件存在，则追加 __dupNNN 直到唯一。
    """
    base = os.path.join(out_dir, base_filename)
    if not os.path.exists(base):
        return base
    stem, ext = os.path.splitext(base_filename)
    ext = ext or ".json"
    n = 1
    while True:
        cand = os.path.join(out_dir, f"{stem}__dup{n:03d}{ext}")
        if not os.path.exists(cand):
            return cand
        n += 1

def natural_key(s: str):
    parts = re.split(r"(\d+)", s)
    return [int(p) if p.isdigit() else p.lower() for p in parts]

def list_json_in_dir(dirpath: str) -> List[str]:
    files = [fn for fn in os.listdir(dirpath) if fn.lower().endswith(".json")]
    files.sort(key=natural_key)
    return [os.path.join(dirpath, fn) for fn in files]

def iter_dir_json_groups(root: str, recursive: bool = True):
    """
    以“目录”为单位产出：(dirpath, [该目录内的 .json 文件列表(已排序)])
    """
    if not recursive:
        yield root, list_json_in_dir(root)
        return

    for dp, _, _ in os.walk(root):
        yield dp, list_json_in_dir(dp)

def is_probably_base64(s: str) -> bool:
    # 简单启发式：字符串很少含 '<'，且 base64 字符集占比高
    if not s or "<" in s[:200]:
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9+/=\s]+", s.strip()))

def try_decode_content(rec: Dict) -> str:
    """
    尝试从记录中拿到 HTML 文本：
    - 优先 rec['content'] / rec['html'] 字符串
    - 如果看起来像 base64，尝试 b64decode -> (gzip|zlib|原文) 解码
    - 全部失败则返回空字符串
    """
    s = rec.get("content") or rec.get("html") or ""
    if not isinstance(s, (str, bytes)):
        return ""

    if isinstance(s, bytes):
        try:
            return s.decode("utf-8", "replace")
        except Exception:
            return ""

    txt = s
    if "<html" in txt.lower() or "<!doctype html" in txt.lower():
        return txt

    if is_probably_base64(txt):
        try:
            raw = base64.b64decode(txt, validate=True)
            try:
                return gzip.decompress(raw).decode("utf-8", "replace")
            except Exception:
                pass
            for wbits in (zlib.MAX_WBITS, -zlib.MAX_WBITS):
                try:
                    return zlib.decompress(raw, wbits).decode("utf-8", "replace")
                except Exception:
                    continue
            try:
                return raw.decode("utf-8", "replace")
            except Exception:
                return ""
        except Exception:
            pass
    return txt or ""


# ---------- 时间解析 ----------
def _normalize_iso_offset(s: str) -> str:
    if not s:
        return s
    s = s.strip()
    if s.endswith("Z"):
        return s[:-1] + "+00:00"
    m = re.search(r"([+-]\d{2})(\d{2})$", s)
    if m:
        s = s[:-5] + m.group(1) + ":" + m.group(2)
    return s

def _to_utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_datetime_to_utc_iso(dt_str: str) -> Optional[str]:
    try:
        iso = _normalize_iso_offset(dt_str)
        dt = datetime.fromisoformat(iso)
        return _to_utc_iso(dt)
    except Exception:
        return None


# ---------- URL / Thread ----------
THREAD_ID_RE = re.compile(r"/threads?/[^./]+\.(\d+)", re.I)

def pick_thread_id(url: str) -> Optional[str]:
    if not url:
        return None
    m = THREAD_ID_RE.search(urlsplit(url).path or "")
    return m.group(1) if m else None

def extract_page_from_url(url: str) -> int:
    if not url:
        return 1
    u = urlsplit(url)
    q = parse_qs(u.query)
    if "page" in q and q["page"]:
        m = re.match(r"^\d+", q["page"][0])
        if m:
            try:
                return int(m.group(0))
            except Exception:
                pass
    m2 = re.search(r"/page-(\d+)(/|$)", u.path or "")
    if m2:
        try:
            return int(m2.group(1))
        except Exception:
            pass
    return 1

def to_abs_url(path_or_url: str) -> str:
    if not path_or_url:
        return ""
    if path_or_url.startswith(("http://","https://")):
        return path_or_url
    return BASE_ORIGIN.rstrip("/") + path_or_url


# ---------- 文本处理 ----------
def text_with_br(el: Optional[Tag]) -> Optional[str]:
    if el is None:
        return None
    tmp = BeautifulSoup(str(el), "html.parser")
    for br in tmp.find_all("br"):
        br.replace_with("\n")
    for bad in tmp.select("script,style,noscript"):
        bad.decompose()
    txt = tmp.get_text("\n", strip=True)
    txt = re.sub(r"[ \t]+\n", "\n", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt).strip()
    return txt or None


# ---------- 帖子解析 ----------
def is_reply_container(tag: Optional[Tag]) -> bool:
    if not isinstance(tag, Tag):
        return False
    classes = set(tag.get("class", []))
    return any(cls in classes for cls in REPLY_CONTAINER_CLASSES)

def nearest_reply_container(tag: Tag) -> Optional[Tag]:
    p = tag.parent
    while isinstance(p, Tag):
        if is_reply_container(p):
            return p
        p = p.parent
    return None

def select_top_level_posts(main_block: Tag) -> List[Tag]:
    posts = []
    for a in main_block.select("article.message"):
        if nearest_reply_container(a) is None:
            posts.append(a)
    return posts

def get_post_id(article: Tag) -> Optional[str]:
    cand = article.get("data-content") or ""
    m = re.search(r"post-(\d+)", cand)
    if m: return m.group(1)
    cand = article.get("id") or ""
    m = re.search(r"post-(\d+)", cand)
    if m: return m.group(1)
    anchor = article.find(id=re.compile(r"post-(\d+)"))
    if anchor and anchor.get("id"):
        m = re.search(r"post-(\d+)", anchor.get("id"))
        if m: return m.group(1)
    a = article.select_one("a[href*='#post-'], a[href*='/post-']")
    if a and a.get("href"):
        m = re.search(r"post-(\d+)", a.get("href"))
        if m: return m.group(1)
    return None

def pick_message_author(article: Tag) -> Optional[str]:
    main = article.find("div", class_=re.compile(r"\bmessage-main\b"))
    scope = main if main else article
    top = scope.select_one("header .message-attribution-user--top .username")
    if top and top.get_text(strip=True):
        return top.get_text(" ", strip=True)
    a = article.get("data-author")
    if a:
        return a
    inner = article.find("div", class_=re.compile(r"\bmessage-inner\b"))
    if inner:
        user_cell = inner.find("div", class_=re.compile(r"\bmessage-cell--user\b"))
        if user_cell:
            u = user_cell.select_one(".message-user .username")
            if u and u.get_text(strip=True):
                return u.get_text(" ", strip=True)
    return None

def pick_message_time_pair(article: Tag) -> Tuple[Optional[str], Optional[str]]:
    main = article.find("div", class_=re.compile(r"\bmessage-main\b"))
    scope = main if main else article
    t = scope.select_one("header time.u-dt")
    if t:
        display = t.get("datetime") or t.get_text(strip=True) or None
        utc_iso = None
        if t.get("datetime"):
            utc_iso = parse_datetime_to_utc_iso(t["datetime"])
        if not utc_iso and t.get("data-time"):
            try:
                secs = int(t["data-time"])
                dt = datetime.fromtimestamp(secs, tz=timezone.utc)
                utc_iso = _to_utc_iso(dt)
            except Exception:
                pass
        return display or None, utc_iso
    md = scope.select_one("header .meta-date")
    if md:
        display = md.get("title") or md.get_text(" ", strip=True) or None
        return display, None
    return None, None

def pick_message_body(article: Tag) -> Optional[str]:
    main = article.find("div", class_=re.compile(r"\bmessage-main\b"))
    search_in = main if main else article
    body = (search_in.select_one(".message-body .bbWrapper")
            or search_in.select_one(".message-content .bbWrapper"))
    if not body:
        body = (search_in.select_one(".message-body")
                or search_in.select_one(".message-content")
                or search_in.select_one(".bbWrapper"))
    return text_with_br(body)

def find_reply_container_after(article: Tag) -> Optional[Tag]:
    for sib in article.next_siblings:
        if isinstance(sib, NavigableString):
            continue
        if isinstance(sib, Tag):
            if is_reply_container(sib):
                return sib
            if sib.name == "article" and "message" in sib.get("class", []):
                return None
    return None

def find_reply_container_for(article: Tag, pid: Optional[str]) -> Optional[Tag]:
    if pid:
        sel = f".js-tprReplyMessageContainer[data-post-id='{pid}']"
        c = article.select_one(sel)
        if c: return c
        c2 = article.find(lambda t: isinstance(t, Tag)
                          and is_reply_container(t)
                          and str(t.get("data-post-id") or "") == str(pid))
        if c2: return c2
    return find_reply_container_after(article)


# ---------- 线程元数据 ----------
def pick_breadcrumb_category_path(soup: BeautifulSoup) -> Optional[str]:
    items = []
    for li in soup.select("ul.p-breadcrumbs li"):
        name = None
        sp = li.select_one("[itemprop='name']")
        if sp and sp.get_text(strip=True):
            name = sp.get_text(" ", strip=True)
        else:
            a = li.find("a")
            if a and a.get_text(strip=True):
                name = a.get_text(" ", strip=True)
        if name:
            items.append(name)
    if not items:
        return None
    if items[0].lower() in ("forums", "home"):
        items = items[1:]
    return " > ".join(items) if items else None

def pick_thread_meta(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    out = dict(
        title_tag=None, thread_title=None, thread_prefix=None,
        thread_starter=None, thread_started_at=None, thread_started_at_utc=None,
        category_path=None
    )
    if soup.title:
        out["title_tag"] = soup.title.get_text(" ", strip=True)

    h1 = soup.select_one("h1.p-title-value")
    if h1:
        out["thread_title"] = h1.get_text(" ", strip=True) or None
        pref = h1.select_one(".label, .labelLink .label")
        if pref:
            out["thread_prefix"] = pref.get_text(" ", strip=True) or None

    desc = soup.select_one(".p-description")
    if desc:
        u = desc.select_one("a.username")
        if u:
            out["thread_starter"] = u.get_text(" ", strip=True) or None
        t = desc.find("time")
        if t:
            dt_raw = t.get("datetime") or t.get_text(strip=True)
            out["thread_started_at"] = dt_raw or None
            if t.get("datetime"):
                out["thread_started_at_utc"] = parse_datetime_to_utc_iso(t["datetime"])

    out["category_path"] = pick_breadcrumb_category_path(soup)
    return out


# ---------- 页面解析（不聚合） ----------
def parse_thread_page(html_text: str, page_url: str) -> Dict:
    html_text = html.unescape((html_text or "").replace(r"\/", "/"))
    soup = BeautifulSoup(html_text, "html.parser")

    meta = pick_thread_meta(soup)
    main_block = (soup.select_one(".block.block--messages")
                  or soup.select_one("div.block-container")
                  or soup.select_one("div[id='posts']"))
    posts: List[Dict] = []

    if main_block:
        top_posts = select_top_level_posts(main_block)
        for art in top_posts:
            process_post_recursive(art, parent_post_id=None, level=0, out_list=posts)
    else:
        # 兜底：抓主要内容块
        for wrap in soup.select(".block--messages .bbWrapper, .message-content .bbWrapper"):
            art = wrap.find_parent("article")
            posted_at, posted_at_utc = pick_message_time_pair(art) if art else (None, None)
            posts.append({
                "post_id": get_post_id(art) if art else None,
                "parent_post_id": None,
                "level": 0,
                "author": pick_message_author(art) if art else None,
                "posted_at": posted_at,
                "posted_at_utc": posted_at_utc,
                "content": text_with_br(wrap)
            })

    return {"url": page_url, "meta": meta, "posts": posts}


def process_post_recursive(article: Tag, parent_post_id: Optional[str], level: int,
                           out_list: List[Dict]):
    pid = get_post_id(article)
    posted_at, posted_at_utc = pick_message_time_pair(article)
    post = {
        "post_id": pid,
        "parent_post_id": parent_post_id,
        "level": level,
        "author": pick_message_author(article),
        "posted_at": posted_at,
        "posted_at_utc": posted_at_utc,
        "content": pick_message_body(article),
    }
    out_list.append(post)

    sub = find_reply_container_for(article, pid)
    if sub is not None:
        for art in sub.find_all("article", class_="message", recursive=True):
            near = nearest_reply_container(art)
            if near is not sub:
                continue
            process_post_recursive(art, parent_post_id=pid, level=level + 1, out_list=out_list)


# ---------- 主流程：逐页写 JSON + 目录级计时 ----------
def main():
    ensure_dir(OUTPUT_DIR)

    # 汇总指标
    grand_scanned = 0
    grand_written = 0
    grand_skipped = 0

    # 罗列所有要跑的目录
    dir_groups = list(iter_dir_json_groups(JSON_DIR, RECURSIVE))
    log.info("输入根目录: %s", JSON_DIR)
    log.info("输出目录  : %s", OUTPUT_DIR)
    log.info("将处理目录数: %d", len(dir_groups))

    for idx, (dirpath, files) in enumerate(dir_groups, start=1):
        if not files:
            continue

        rel = os.path.relpath(dirpath, JSON_DIR)
        dir_scanned = 0
        dir_written = 0
        dir_skipped = 0

        log.info("📂 [%d/%d] 开始处理目录: %s  (JSON: %d)",
                 idx, len(dir_groups), rel if rel != "." else ".", len(files))
        t0 = time.perf_counter()

        # 每个目录内按自然序处理
        for jp in files:
            dir_scanned += 1
            grand_scanned += 1

            try:
                with open(jp, "r", encoding=ENCODING) as f:
                    rec = json.load(f)
            except Exception as e:
                log.error("[打开失败] %s: %s", jp, e)
                continue

            url = rec.get("url") or rec.get("redirected_url") or ""
            if not THREAD_PATH_FILTER.search((urlsplit(url).path or "").lower()):
                dir_skipped += 1
                grand_skipped += 1
                continue

            html_text = try_decode_content(rec)
            if not html_text:
                log.warning("[空内容] %s", jp)
                dir_skipped += 1
                grand_skipped += 1
                continue

            page_no = extract_page_from_url(url)
            tid = pick_thread_id(url) or "NA"

            try:
                page_data = parse_thread_page(html_text, url)
            except Exception as e:
                log.error("[解析失败] %s: %s", jp, e)
                dir_skipped += 1
                grand_skipped += 1
                continue

            title_slug = sanitize(
                page_data.get("meta", {}).get("thread_title")
                or page_data.get("meta", {}).get("title_tag")
                or "thread"
            )
            base_name = f"tid_{tid}__p{page_no:03d}__{title_slug}.json"
            out_path = make_unique_json_path(OUTPUT_DIR, base_name)

            out_obj = {
                "url": url,
                "thread_id": tid if tid != "NA" else None,
                "page_no": page_no,
                "meta": page_data.get("meta") or {},
                "posts": page_data.get("posts") or []
            }
            try:
                with io.open(out_path, "w", encoding=ENCODING) as wf:
                    wf.write(json.dumps(out_obj, ensure_ascii=False, indent=2))
                dir_written += 1
                grand_written += 1
                log.info("[OK] %s  ->  %s  (帖子:%d)",
                         os.path.basename(jp), os.path.basename(out_path),
                         len(out_obj["posts"]))
            except Exception as e:
                log.error("[写文件失败] %s: %s", out_path, e)

        elapsed = time.perf_counter() - t0
        log.info("✅ 目录完成: %s | 扫描:%d 写出:%d 跳过:%d | 耗时: %.2fs",
                 rel if rel != "." else ".", dir_scanned, dir_written, dir_skipped, elapsed)

    # 汇总
    log.info("===== 汇总 =====")
    log.info("总扫描 JSON : %d", grand_scanned)
    log.info("总写出页面  : %d", grand_written)
    log.info("总跳过      : %d", grand_skipped)
    log.info("输出目录    : %s", OUTPUT_DIR)


if __name__ == "__main__":
    main()
