# Parallel Web Scraper with Selenium + Trafilatura

A **high-performance parallel web crawler** built with **Python, Selenium, and Trafilatura** that extracts **clean, structured text from websites**.
It is designed for **LLM dataset creation, knowledge base building, and large-scale content extraction**.

The crawler loads pages in **parallel using multiple Chrome instances**, extracts readable text, cleans it for **LLM pipelines**, and saves results in **JSONL and TXT formats**.

---

## ✨ Features

🔹 **Parallel Crawling**
Uses `ThreadPoolExecutor` to crawl multiple pages simultaneously.

🔹 **Thread-Local Chrome Drivers**
Each thread maintains its own Chrome driver for **maximum efficiency and stability**.

🔹 **Smart URL Normalization**
Prevents duplicate crawling by canonicalizing URLs.

🔹 **Same-Domain Crawling**
Ensures the crawler **stays within the target website**.

🔹 **Robust Text Extraction**

* Primary extraction using **Trafilatura**
* Automatic **fallback extraction** for difficult pages

🔹 **Advanced Text Cleaning Pipeline**

* Removes invisible characters
* Deduplicates repeated content
* Normalizes Unicode
* Produces **LLM-ready text**

🔹 **Metadata-Rich Output**
Each article includes:

* URL
* Title
* Timestamp
* Word count
* Clean article text

🔹 **Structured Output**

* `JSONL` → ideal for **LLM training pipelines**
* `TXT` → human-readable archive

---

# 🧠 How It Works

The crawler follows a **parallel Breadth-First Search (BFS)** strategy:

1. Start from a **seed URL**
2. Load pages with **Selenium**
3. Extract content with **Trafilatura**
4. Collect new links from the page
5. Normalize and filter URLs
6. Add them to the crawling frontier
7. Continue until:

   * the site is fully explored, or
   * the **maximum article limit** is reached

This architecture keeps **all threads busy** for maximum throughput.

---

# 🏗️ Project Structure

```
web_scraper.py
```

Main components:

| Component         | Description                               |
| ----------------- | ----------------------------------------- |
| Driver Management | Thread-local Chrome drivers               |
| URL Utilities     | URL normalization and filtering           |
| Text Extraction   | Trafilatura primary + fallback extraction |
| Text Cleaning     | LLM-ready text preprocessing              |
| Parallel Crawler  | BFS crawler using ThreadPoolExecutor      |
| Output Writers    | JSONL and TXT export                      |

---

# ⚙️ Installation

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/your-username/parallel-web-scraper.git
cd parallel-web-scraper
```

---

### 2️⃣ Install Dependencies

```bash
pip install selenium trafilatura webdriver-manager
```

---

### 3️⃣ Install Google Chrome

The crawler uses **headless Chrome**.

Download Chrome if needed:

https://www.google.com/chrome/

---

# ▶️ Usage

Run the scraper:

```bash
python web_scraper.py
```

You will be prompted for:

```
Enter root URL to crawl:
Enter maximum articles to save (blank = unlimited):
```

Example:

```
Enter root URL to crawl: https://example.com
Enter maximum articles to save: 50
```

---

# 📂 Output Files

After crawling finishes, two files are generated.

### 📄 JSONL Output

```
docs_<site>.jsonl
```

Example record:

```json
{
"url": "https://example.com/blog/post/",
"title": "Blog Post",
"scraped_at": "2026-03-10T10:20:00Z",
"word_count": 1200,
"text": "Clean extracted article text..."
}
```

Perfect for:

* LLM training datasets
* Retrieval systems
* vector databases

---

### 📑 TXT Output

```
docs_<site>.txt
```

Human-readable format:

```
ARTICLE [1 / 10]
Title : Example Post
URL   : https://example.com/post

Article text...
```

---

# ⚡ Performance Optimizations

The scraper is optimized for **speed and stability**.

Key techniques used:

✔ Headless Chrome
✔ Disabled image loading
✔ Thread-local drivers
✔ Parallel execution
✔ Smart waiting (no unnecessary sleeps)
✔ Retry mechanism with exponential backoff

---

# 🧹 Text Cleaning for LLMs

Extracted text passes through a **multi-stage cleaning pipeline**:

* Unicode normalization
* Control character removal
* Whitespace normalization
* Decorative line removal
* Duplicate block removal
* Blank line collapsing

Result: **high-quality LLM-ready text**.

---

# 🔧 Configuration

Important parameters inside `web_scraper.py`:

```python
PARALLEL_WORKERS = 4
PAGE_TIMEOUT = 60
MAX_RETRIES = 2
REQUEST_DELAY = 0.0
```

You can adjust these depending on:

* website speed
* rate limits
* hardware capability

---

# 🛑 Respect Website Policies

Always respect:

* `robots.txt`
* website **terms of service**
* server **rate limits**

Use responsibly.

---

# 📈 Example Use Cases

📚 Build **LLM training datasets**
🔍 Create **search indexes**
📊 Collect data for **research**
🧠 Knowledge base generation
📄 Documentation scraping

---

# 🧑‍💻 Author

**Virendra Badgotya**
AI/ML Enthusiast | Python Developer

GitHub:
https://github.com/vir123-devf

LinkedIn:
https://www.linkedin.com/in/virendra-badgotya/

---

# ⭐ If You Found This Useful

Give the repository a ⭐ on GitHub to support the project!
