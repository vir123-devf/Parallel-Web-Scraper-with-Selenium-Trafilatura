# web_scraper.py
""" 
A parallel web crawler that uses Selenium and trafilatura to extract clean text from a website.
Features:       
- Configurable root URL and article cap        
- ThreadPoolExecutor for concurrent page loads  
- Thread-local Chrome drivers for efficient reuse
- Robust URL normalization and filtering to stay on-site
- Primary + fallback extraction strategies for maximum text retrieval
- Comprehensive text cleaning pipeline for LLM readiness
- Structured record building with metadata
- JSONL output for LLM pipelines and human-readable TXT output
"""
# ── Standard library ──────────────────────────────────────────────────────────
from __future__ import annotations  # allow 'type | None' syntax on Python 3.9
import concurrent.futures   # ThreadPoolExecutor + Future management
import json                 # serialise records to JSONL
import os                   # cpu_count for auto-sizing worker pool
import re                   # regex for text cleaning and URL slug extraction
import sys                  # stdout/stderr reconfiguration
import threading            # thread-local storage + locks
import unicodedata          # unicode normalisation in text cleaning
from datetime import datetime, timezone   # UTC timestamps on records
from pathlib import Path                  # cross-platform file paths
from urllib.parse import urlparse, urlunparse  # URL normalisation helpers
import time                 

# ── Third-party ───────────────────────────────────────────────────────────────
import trafilatura                          # primary HTML → clean-text extractor
from selenium import webdriver             # browser automation
from selenium.webdriver.chrome.options import Options   # headless Chrome flags
from selenium.webdriver.chrome.service import Service   # driver process wrapper
from selenium.webdriver.common.by import By             # element selector enum
from selenium.webdriver.support import expected_conditions as EC  # wait helpers
from selenium.webdriver.support.ui import WebDriverWait  # explicit waits
from webdriver_manager.chrome import ChromeDriverManager  # auto-download driver

start_time = time.time() # Recording start time for total execution duration reporting at the end


# ══════════════════════════════════════════════════════════════════════════════
#  I/O helpers
# ══════════════════════════════════════════════════════════════════════════════

def _configure_stdio() -> None:
    """
    Force stdout and stderr to UTF-8 with replacement for unencodable chars.
    Prevents UnicodeEncodeError when printing scraped content on Windows
    terminals.
    """
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass  # silently ignore if reconfigure is unavailable (e.g. redirected pipe)

_configure_stdio()


# ══════════════════════════════════════════════════════════════════════════════
#  Runtime configuration
# ══════════════════════════════════════════════════════════════════════════════

TARGET_URL: str = ""          # root URL supplied by the user
MAX_PAGES: int | None = None  # stop after this many *saved* articles (None = unlimited)
PARALLEL_WORKERS: int = 4     # concurrent Chrome threads.


def url_to_title(url: str) -> str:
    """
    Derive a human-readable title from the last path segment of a URL.
    Example:
        'https://example.com/blog/2024/06/fast-crawler' -> 'Fast Crawler'
    """
    slug = urlparse(url).path.strip("/").split("/")[-1]   # grab last path part
    slug = re.sub(r"[-_]+", " ", slug)                    # hyphens/underscores -> spaces
    return slug.title() or url                            # retrieved title or fallback to URL if path is empty


# Output filenames:
OUTPUT_JSONL = f"docs_{url_to_title(TARGET_URL)}.jsonl"  # json records
OUTPUT_TXT   = f"docs_{url_to_title(TARGET_URL)}.txt"    # text file

# Per-page delay injected AFTER page load (0 = disabled).
# Increase only if the target site returns 429 / rate-limit errors.
REQUEST_DELAY: float = 0.0

# Selenium page-load timeout in seconds(Raised to 60 to handle slow JS apps).
PAGE_TIMEOUT: int = 60

# How many times to retry a failed page load before giving up.
MAX_RETRIES: int = 2

# CSS selectors used to select all the links.
LINK_SELECTORS = ["a[href]"]

# Resolved once in main().
_DRIVER_PATH: str = ""


# ══════════════════════════════════════════════════════════════════════════════
#  Chrome driver helpers
# ══════════════════════════════════════════════════════════════════════════════

def _chrome_options() -> Options:
    """
    Build a headless Chrome Options object tuned for fast scraping.

    Flags chosen for speed / stealth:
      --headless=new            : new headless mode (more stable than legacy)
      --no-sandbox              : required in Docker/CI environments
      --disable-dev-shm-usage   : avoids /dev/shm OOM crashes in containers
      --disable-gpu             : not needed in headless mode
      --disable-images          : skip image downloads -> faster page loads
      --disable-extensions      : no unnecessary extension overhead
      excludeSwitches / useAutomationExtension: hide automation fingerprints
    """
    o = Options()
    o.add_argument("--headless=new")
    o.add_argument("--no-sandbox")
    o.add_argument("--disable-dev-shm-usage")
    o.add_argument("--disable-gpu")
    o.add_argument("--window-size=1920,1080")
    o.add_argument("--disable-extensions")
    o.add_argument("--disable-images")                        # skip image bytes
    o.add_argument("--blink-settings=imagesEnabled=false")    # belt-and-suspenders for images
    o.add_argument("--disable-javascript-harmony-shipping")   # skip experimental JS features
    # Mimic a real browser so sites don't serve bot-detection pages
    o.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    o.add_argument("--disable-blink-features=AutomationControlled")      # hide webdriver flag
    o.add_experimental_option("excludeSwitches", ["enable-automation"])  # remove automation bar
    o.add_experimental_option("useAutomationExtension", False)           # disable automation ext
    return o


def _make_driver() -> webdriver.Chrome:
    """
    Instantiate a single Chrome WebDriver using the pre-resolved driver binary.

    Uses the module-level _DRIVER_PATH (set once in main) so threads never
    trigger redundant ChromeDriverManager network/filesystem calls.
    """
    service = Service(_DRIVER_PATH)                          # point to cached binary
    driver = webdriver.Chrome(service=service, options=_chrome_options())
    driver.set_page_load_timeout(PAGE_TIMEOUT)               # raise TimeoutException if exceeded
    return driver


# ── Thread-local driver pool ──────────────────────────────────────────────────
# threading.local() gives each thread its own isolated namespace.
# _tls.driver holds the Chrome instance that belongs to that thread.
_tls = threading.local()


def _get_driver() -> webdriver.Chrome:
    """
    Return the Chrome driver for the current thread, creating it on first call.

    Thread-local reuse means:
      - No Chrome startup/teardown between pages on the same thread
      - No shared state between threads (each thread has its own browser session)
    """
    if not getattr(_tls, "driver", None):   # first call on this thread
        _tls.driver = _make_driver()
    return _tls.driver


def _close_thread_driver() -> None:
    """
    Gracefully quit the Chrome instance owned by the current thread.
    Called during cleanup; silently ignores errors (driver may already be dead).
    """
    driver = getattr(_tls, "driver", None)
    if driver:
        try:
            driver.quit()
        except Exception:
            pass            # ignore stale/crashed driver
        _tls.driver = None  # mark as gone so _get_driver() will recreate if needed


# ══════════════════════════════════════════════════════════════════════════════
#  URL helpers
# ══════════════════════════════════════════════════════════════════════════════

def normalise_url(url: str) -> str:
    """
    Canonicalise a URL so that equivalent URLs map to the same string key.

    Transformations applied:
      - scheme and host lowercased          ('HTTP://Example.com' -> 'http://example.com')
      - trailing slash added to path        ('/blog' -> '/blog/')
      - query string and fragment stripped  ('?utm_source=...' and '#section' removed)

    This prevents the crawler from visiting the same page multiple times
    under superficially different URLs.
    """
    parsed = urlparse(url)
    canonical = urlunparse((
        parsed.scheme.lower(),           # normalise scheme case
        parsed.netloc.lower(),           # normalise host case
        parsed.path.rstrip("/") + "/",   # ensure trailing slash
        "",                              # drop params
        "",                              # drop query string
        "",                              # drop fragment
    ))
    return canonical


def is_crawlable(url: str) -> bool:
    """
    Decide whether a URL should be added to the crawl frontier.

    A URL is accepted only when ALL of the following hold:
      1. Both url and TARGET_URL are non-empty strings
      2. Scheme is http or https  (rejects mailto:, javascript:, etc.)
      3. URL has a non-empty netloc (rejects relative URLs that slipped through)
      4. Netloc matches TARGET_URL's domain exactly (stay on-site)
      5. URL has a non-empty path segment (rejects bare domain roots)
    """
    if not url or not TARGET_URL:
        return False                          # guard against empty input

    target_parsed = urlparse(TARGET_URL)
    url_parsed    = urlparse(url)

    if url_parsed.scheme not in ("http", "https"):
        return False                            # non-web schemes (mailto:, javascript:, etc.)

    if not url_parsed.netloc:
        return False                            # relative or malformed URL

    if url_parsed.netloc.lower() != target_parsed.netloc.lower():
        return False                            # external domain -- skip

    if not url_parsed.path.strip("/"):
        return False                            # bare domain root -- skip

    return True


# ══════════════════════════════════════════════════════════════════════════════
#  Text extraction  (via trafilatura)
# ══════════════════════════════════════════════════════════════════════════════

def extract_text_primary(html: str, url: str) -> str | None:
    """
    Primary extraction path using trafilatura with strict settings.

    Parameters:
        html : raw page HTML string from Selenium
        url  : canonical URL of the page (used by trafilatura for heuristics)

    Returns:
        Extracted plain text, or None if trafilatura found nothing worth keeping.

    trafilatura settings:
        no_fallback=True    -> strict mode; return None rather than guess
        favor_recall=True   -> prefer more text over higher precision
        include_tables=True -> capture tabular data as text
        deduplicate=True    -> suppress repeated boilerplate blocks
    """
    try:
        return trafilatura.extract(
            html,
            url=url,
            include_comments=False,   # skip comment sections
            include_tables=True,      # include table content
            no_fallback=True,         # strict: return None rather than guess
            favor_recall=True,        # lean toward more content
            include_images=True,      # include image alt-text in output
            deduplicate=True,         # remove repeated navigation/footer blurbs
        )
    except Exception as exc:
        print(f"    [WARN] Primary extraction failed for {url}: {exc}")
        return None  # nothing could be extracted


def extract_text_fallback(html: str, url: str) -> str | None:
    """
    Fallback extraction when the primary (strict) pass returns None.

    Strategy:
      1. Retry trafilatura with no_fallback=False to enable its internal
         heuristic mode -- less precise but rescues non-standard page layouts.
      2. If that still fails, use trafilatura.bare_extraction() which returns
         a dict with a 'text' key containing raw visible body text.

    Returns:
        Extracted plain text, or None if all methods fail.
    """
    try:
        # Re-try trafilatura with its own internal fallback heuristics enabled
        text = trafilatura.extract(
            html,
            url=url,
            include_comments=False,
            include_tables=True,
            no_fallback=False,    # allow trafilatura's built-in fallback
            favor_recall=True,
            include_images=True,
            deduplicate=True,
        )
        if text:
            return text

        # Last resort: bare_extraction returns a structured dict; grab 'text' key
        bare = trafilatura.bare_extraction(html, url=url, include_tables=True)
        if bare and bare.get("text"):
            return bare["text"]

        return None  # nothing could be extracted

    except Exception as exc:
        print(f"    [WARN] Fallback extraction failed for {url}: {exc}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  Text cleaning
# ══════════════════════════════════════════════════════════════════════════════

def clean_text_for_llm(raw: str) -> str:
    """
    Post-process raw extracted text into a clean, LLM-friendly string.

    Steps applied in order:
      1. Unicode NFC normalisation -- composites -> precomposed characters
      2. Strip invisible / control characters (zero-width spaces, BOM, etc.)
      3. Collapse repeated whitespace within lines; trim each line
      4. Remove pure decorator lines (---, ===, ###, etc.)
      5. Collapse runs of blank lines to at most one blank line
      6. Deduplicate adjacent repeated blocks to remove copy-pasted boilerplate

    Returns:
        Cleaned text ending with a single newline character.
    """
    # NFC normalisation so accented letters are single code points
    text = unicodedata.normalize("NFC", raw)

    # Remove invisible / control characters that confuse tokenisers
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\u200b-\u200f\ufeff]", "", text)

    lines = text.splitlines()
    cleaned: list[str] = []
    blank_run = 0  # track consecutive blank lines to enforce the one-blank-line rule

    for line in lines:
        # Collapse inner whitespace and trim leading/trailing spaces
        line = re.sub(r"[ \t]+", " ", line).strip()

        # Drop lines that contain only decorator characters (e.g. '---')
        if re.fullmatch(r"[-=*_#~`]{3,}", line):
            continue

        if line == "":
            blank_run += 1
            if blank_run <= 1:          # allow at most one consecutive blank line
                cleaned.append("")
        else:
            blank_run = 0
            cleaned.append(line)

    # sliding-window duplicate-block removal
    # If cleaned[i : i+w] == cleaned[i+w : i+2w], the block is a duplicate -> emit once
    out: list[str] = []
    i = 0
    n = len(cleaned)
    max_span = 200  # maximum window size (lines) to check for duplication

    while i < n:
        max_w = min(max_span, (n - i) // 2)  # can't check beyond end of list
        removed = False

        for w in range(max_w, 0, -1):
            # Check whether the next 'w' lines are immediately repeated
            if cleaned[i : i + w] == cleaned[i + w : i + 2 * w]:
                out.extend(cleaned[i : i + w])  # keep the block once
                i += 2 * w                       # skip past the duplicate copy
                removed = True
                break

        if not removed:
            out.append(cleaned[i])  # no duplicate found; emit line as-is
            i += 1

    return "\n".join(out).strip() + "\n"  # single trailing newline for consistency


def build_record(url: str, text: str, depth: int = 0) -> dict:
    """
    Assemble a structured record dict from a page URL and its extracted text.

    Fields:
        url          : canonical page URL
        title        : human-readable title derived from the URL path
        scraped_at   : ISO-8601 UTC timestamp of when the page was scraped
        crawl_depth  : BFS depth (always 0 currently; reserved for future use)
        word_count   : whitespace-token count of the cleaned text
        text         : cleaned plain text ready for LLM ingestion
    """
    clean = clean_text_for_llm(text)       # run full cleaning pipeline
    return {
        "url":         url,
        "title":       url_to_title(url),
        "scraped_at":  datetime.now(timezone.utc).isoformat(),
        # "crawl_depth": depth,
        "word_count":  len(clean.split()), 
        "text":        clean
    }


# ══════════════════════════════════════════════════════════════════════════════
#  Page scraper  (called by each worker thread)
# ══════════════════════════════════════════════════════════════════════════════

def _wait_for_page(driver: webdriver.Chrome) -> None:
    """
    Wait for the page to be visually ready without using a fixed sleep.

    Strategy (fastest-first):
      1. WebDriverWait until <body> exists  -- already instant on loaded pages
      2. Poll document.readyState until 'complete'  -- waits for JS to settle
      3. 0.1 s micro-sleep between polls  -- avoids busy-looping the CPU
      4. Hard cap of 5 extra seconds  -- prevents hanging on infinite loaders

    """
    # Block until <body> tag appears (fast if page is already loaded)
    WebDriverWait(driver, PAGE_TIMEOUT).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )

    # Poll readyState for up to 5 additional seconds to let JS finish
    deadline = time.time() + 5
    while time.time() < deadline:
        state = driver.execute_script("return document.readyState")
        if state == "complete":
            break           # page fully loaded -- stop polling
        time.sleep(0.1)     # short pause to avoid busy-loop


def scrape_page(url: str) -> tuple[str | None, set[str]]:
    """
    Load *url* in the calling thread's Chrome instance, then extract text + links.

    Behaviour:
      - Pulls Chrome driver from thread-local pool (creates one on first call)
      - Retries up to MAX_RETRIES times with exponential back-off on load failure
      - Tries strict trafilatura extraction first, then permissive fallback
      - Harvests all <a href> elements for the BFS frontier
      - Applies REQUEST_DELAY (if > 0) after a successful load

    Returns:
        Tuple of:
          raw_text  : extracted plain text, or None if the page yielded nothing
          new_links : set of normalised same-domain URLs found on this page
    """
    driver = _get_driver()  # retrieve this thread's Chrome

    # ── Page load with retry + exponential back-off ───────────────────────────
    for attempt in range(MAX_RETRIES + 1):
        try:
            driver.get(url)          # navigate Chrome to the target URL
            _wait_for_page(driver)   # smart wait for DOM to settle
            break                    # success -- exit retry loop
        except Exception as exc:
            if attempt < MAX_RETRIES:
                wait_time = 2 ** attempt  # 1 s, 2 s, 4 s ...
                print(f"    [RETRY {attempt+1}/{MAX_RETRIES}] {url}  ({exc})")
                time.sleep(wait_time)
            else:
                # All retries exhausted -- give up on this URL
                print(f"    [ERROR] Load failed after {MAX_RETRIES} retries: {url}")
                return None, set()

    # ── Extract text from the rendered HTML ───────────────────────────────────
    html = driver.page_source          # full rendered HTML

    raw_text = extract_text_primary(html, url)   # strict trafilatura pass

    if not raw_text:
        # Primary returned nothing -- try secorndary fallback extraction 
        raw_text = extract_text_fallback(html, url)
        if raw_text:
            print(f"    [INFO] Fallback extraction succeeded for {url}")

    # ── Harvest outbound links for the BFS frontier ───────────────────────────
    raw_links: set[str] = set()
    for selector in LINK_SELECTORS:
        try:
            for element in driver.find_elements(By.CSS_SELECTOR, selector):
                href = element.get_attribute("href")  # Selenium resolves relative hrefs
                if href:
                    raw_links.add(href)
        except Exception:
            pass  # ignore stale element or driver errors during link harvest

    # Filter to same-domain URLs and normalise them for deduplication
    new_links = {normalise_url(u) for u in raw_links if is_crawlable(u)}

    # Optional courtesy delay (disabled by default; raise REQUEST_DELAY if throttled)
    if REQUEST_DELAY > 0:
        time.sleep(REQUEST_DELAY)

    return raw_text, new_links


# ══════════════════════════════════════════════════════════════════════════════
#  Single-pass concurrent BFS crawl
# ══════════════════════════════════════════════════════════════════════════════

def crawl_parallel() -> list[dict]:
    """
    Crawl the site from TARGET_URL using a live parallel BFS strategy.

    Architecture
    ────────────
    - ThreadPoolExecutor manages PARALLEL_WORKERS threads.
    - Each thread owns one Chrome instance (thread-local) reused across pages.
    - `inflight` dict maps each live Future to the URL it is processing.
    - When a Future completes:
        * its result (text + new links) is processed immediately
        * newly discovered links are submitted as new Futures right away
      This keeps workers busy without a separate sequential discovery phase.
    - Crawl terminates when either:
        a) the frontier is exhausted (all reachable pages visited), or
        b) len(records) >= MAX_PAGES articles have been saved

    Thread-safety
    ─────────────
    Every shared mutable variable has a dedicated lock:
        seen_lock           -> `seen` set of visited URLs
        records_lock        -> `records` list of saved articles
        pages_visited_lock  -> page counter used for log numbering
        urls_submitted_lock -> submission counter used for MAX_PAGES cap

    Returns:
        List of record dicts, one per successfully extracted page.
    """
    seed = normalise_url(TARGET_URL)   # canonical starting URL

    # ── Shared state ──────────────────────────────────────────────────────────
    seen:          set[str]   = {seed}  # URLs already submitted (prevents re-visits)
    records:       list[dict] = []      # accumulated extraction results
    pages_visited: int = 0              # total page attempts (for log prefix)
    urls_submitted: int = 0             # total futures submitted (for MAX_PAGES cap)

    # One lock per mutable shared variable
    seen_lock          = threading.Lock()
    records_lock       = threading.Lock()
    pages_visited_lock = threading.Lock()
    urls_submitted_lock = threading.Lock()

    print(f"\n[CRAWL] Starting from: {seed}")
    print(f"[CRAWL] Workers: {PARALLEL_WORKERS}  |  Target articles: {MAX_PAGES or 'unlimited'}\n")

    with concurrent.futures.ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:

        # inflight: Future -> URL mapping for in-progress scrape tasks
        inflight: dict[concurrent.futures.Future, str] = {}

        def _submit(url: str) -> None:
            """Submit a scrape_page task to the thread pool and track it."""
            nonlocal urls_submitted
            future = executor.submit(scrape_page, url)  # schedule on a worker thread
            inflight[future] = url                       # remember URL for this future
            with urls_submitted_lock:
                urls_submitted += 1                      # track total submissions

        def _under_cap() -> bool:
            """Return True if we have not yet reached the MAX_PAGES submission cap."""
            if MAX_PAGES is None:
                return True                             # unlimited mode
            with urls_submitted_lock:
                return urls_submitted < MAX_PAGES

        _submit(seed)  # Start the crawl with the seed URL

        # ── Event loop: process completed futures and submit new ones ──────────
        while inflight:
            # Check if we've already saved enough articles and queue is empty
            with records_lock:
                articles_saved = len(records)

            if MAX_PAGES is not None and articles_saved >= MAX_PAGES and not inflight:
                break  # target reached and no pending futures

            # Block until at least one future finishes (avoids busy-waiting)
            done, _ = concurrent.futures.wait(
                list(inflight.keys()),
                return_when=concurrent.futures.FIRST_COMPLETED,
            )

            for future in done:
                url = inflight.pop(future)   # deregister from inflight tracking

                # Increment page counter automically and snapshot current value
                with pages_visited_lock:
                    pages_visited += 1
                    page_num = pages_visited

                # Retrieve scrape result (exceptions are caught and logged)
                try:
                    raw_text, new_links = future.result()
                except Exception as exc:
                    print(f"[{page_num:>4}]  ERROR  {url}: {exc}")
                    continue

                print(f"[{page_num:>4}]  {url} , no. of new links added: {len(new_links)}")

                # Persist article if text was successfully extracted
                if raw_text:
                    record = build_record(url, raw_text)
                    with records_lock:
                        records.append(record)
                    print(f"         OK  {record['word_count']:,} words saved")
                else:
                    print("         --  skipped (no usable content extracted)")

                # ── Feed newly discovered links into the live pool ─────────────
                with seen_lock:
                    for link in new_links:
                        if link not in seen:       # skip already-queued URLs
                            seen.add(link)
                            if _under_cap():        # Restrict MAX_PAGES cap
                                _submit(link)
                            else:
                                # Remove from `seen` so it stays accurate
                                seen.discard(link)

    with records_lock:
        total_saved = len(records)

    print(f"\n[CRAWL] Done -- {pages_visited} page(s) visited, {total_saved} article(s) saved")
    return records


# ══════════════════════════════════════════════════════════════════════════════
#  Output writers
# ══════════════════════════════════════════════════════════════════════════════

def write_jsonl(records: list[dict], path: Path) -> None:
    """
    Write records as newline-delimited JSON (JSONL) for LLM pipeline ingestion.

    Each line is a self-contained JSON object so the file can be streamed or
    processed incrementally.  Compact separators reduce file size;
    ensure_ascii=False preserves non-ASCII characters.
    """
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            # One JSON object per line -- easy to stream/parse incrementally
            fh.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
    print(f"  [OK] JSONL -> {path.resolve()}  ({len(records)} records)")


def write_txt(records: list[dict], path: Path) -> None:
    """
    Write records as a human-readable text file with header and per-article banners.

    File structure:
        ══ (header bar)
          Generated, Seed URL, Article count
        ══ (bar)

        ══ (bar)
          ARTICLE [1 / N]
          Title, URL
        ══ (bar)
        <article text>
        ... repeated for each article ...
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    bar   = "=" * 78   # full-width separator line
    total = len(records)

    with path.open("w", encoding="utf-8") as fh:
        # File-level header block
        fh.write(bar + "\n")
        fh.write(f"  Generated : {timestamp}\n")
        fh.write(f"  Seed URL  : {TARGET_URL}\n")
        fh.write(f"  Articles  : {total}\n")
        fh.write(bar + "\n")

        # One banner + text block per article
        for idx, record in enumerate(records, start=1):
            fh.write(f"\n{bar}\n")
            fh.write(f"  ARTICLE [{idx} / {total}]\n")
            fh.write(f"  Title  : {record['title']}\n")
            fh.write(f"  URL    : {record['url']}\n")
            fh.write(f"{bar}\n\n")
            fh.write(record["text"]) 

    print(f"  [OK] TXT  -> {path.resolve()}  ({total} articles)")


# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """
    Interactive entry point:
      1. Prompt user for root URL and optional article cap
      2. Resolve ChromeDriver binary path ONCE (shared by all threads)
      3. Auto-size worker count to available CPU cores
      4. Print configuration summary
      5. Run the parallel BFS crawl
      6. Write JSONL and TXT output files
      7. Print total execution time
    """
    global TARGET_URL, MAX_PAGES, PARALLEL_WORKERS, _DRIVER_PATH

    # ── Collect user input ────────────────────────────────────────────────────
    TARGET_URL = input("Enter root URL to crawl: ").strip()
    if TARGET_URL and "://" not in TARGET_URL:
        TARGET_URL = "https://" + TARGET_URL   # default to HTTPS if no scheme given

    max_raw = input("Enter maximum articles to save (blank = unlimited): ").strip()
    MAX_PAGES = int(max_raw) if max_raw.isdigit() else None

    # ── Resolve ChromeDriver once ──────────────────────────────────────────────
    """
    ChromeDriverManager downloads / locates the correct chromedriver binary.
    and all worker threads share the cached path without hitting the filesystem or network repeatedly.
    """
    print("\n[INIT] Resolving ChromeDriver...")
    _DRIVER_PATH = ChromeDriverManager().install()
    print(f"[INIT] Driver -> {_DRIVER_PATH}")

    # ── Auto-size worker pool ──────────────────────────────────────────────────
    # Chrome is I/O-bound (network waits dominate), so one thread per CPU core
    # is a reasonable default that keeps all cores busy.
    PARALLEL_WORKERS = 4

    
    print(f"[INIT] Workers: {PARALLEL_WORKERS}")

    # ── Print configuration summary ────────────────────────────────────────────
    print("\n" + "=" * 78)
    print("Parallel Web Crawler")
    print("=" * 78)
    print(f"  Root URL : {TARGET_URL}")
    print(f"  Max URLs : {MAX_PAGES }")
    print(f"  Workers  : {PARALLEL_WORKERS}")
    print(f"  Delay    : {REQUEST_DELAY} s/page")
    print(f"  Outputs  : {OUTPUT_TXT}  +  {OUTPUT_JSONL}")
    print("=" * 78 + "\n")

    # ── Run crawl ──────────────────────────────────────────────────────────────
    records: list[dict] = []
    try:
        records = crawl_parallel()
    except KeyboardInterrupt:
        # Ctrl-C received -- save whatever was collected before stopping
        print("\n[WARN] Interrupted by user -- saving partial results ...")
    finally:
        # Always attempt to write output if any records were collected
        if records:
            print(f"\n[OUTPUT] Writing {len(records)} record(s) ...")
            write_jsonl(records, Path(OUTPUT_JSONL))
            write_txt(records,  Path(OUTPUT_TXT))
        else:
            print("\n[WARN] No records collected -- nothing to write")

    # ── Total execution time ──────────────────────────────────────────────────────
    elapsed = time.time() - start_time
    print(f"\n[DONE] Total execution time: {elapsed:.1f} s")

if __name__ == "__main__":
    main()