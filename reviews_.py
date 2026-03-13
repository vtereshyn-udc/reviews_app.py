"""
ETL: Apify (junglee/amazon-reviews-scraper) → PostgreSQL
=========================================================
Запуск:
    pip install apify-client psycopg2-binary python-dotenv
    python etl_apify_to_postgres.py

.env (поруч з файлом):
    APIFY_TOKEN=apify_api_xxxxxxxxxxxxxxxx
    PG_HOST=your-vps-ip
    PG_PORT=5432
    PG_DB=your_db
    PG_USER=your_user
    PG_PASSWORD=your_password
"""

import os
import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from apify_client import ApifyClient
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Конфіг ──────────────────────────────────────────────────────────────────

APIFY_TOKEN = os.getenv("APIFY_TOKEN")

PG_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5432)),
    "dbname":   os.getenv("PG_DB", "amazon"),
    "user":     os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),
}

# ASINи + домени для скрейпінгу
# Формат: {"asin": "B0XXXXX", "domain": "amazon.com"}
TARGETS = [
    {"asin": "B0XXXXXXXXX", "domain": "amazon.com"},
    {"asin": "B0XXXXXXXXX", "domain": "amazon.de"},
    # додай свої...
]

ACTOR_ID = "junglee/amazon-reviews-scraper"

# ── DDL (створення таблиці) ──────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS reviews (
    id                 BIGSERIAL PRIMARY KEY,
    asin               TEXT        NOT NULL,
    domain             TEXT        NOT NULL,
    review_id          TEXT        UNIQUE,          -- унікальний ID від Amazon
    rating             SMALLINT,
    title              TEXT,
    content            TEXT,
    author             TEXT,
    author_url         TEXT,
    is_verified        BOOLEAN     DEFAULT FALSE,
    review_date        DATE,
    helpful_votes      INTEGER     DEFAULT 0,
    product_attributes TEXT,                        -- "Size: M, Color: Red"
    product_variant    TEXT,
    images             JSONB,                       -- масив URL зображень у відгуку
    raw_json           JSONB,                       -- повний оригінальний об'єкт
    scraped_at         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(asin, domain, review_id)
);

CREATE INDEX IF NOT EXISTS idx_reviews_asin   ON reviews(asin);
CREATE INDEX IF NOT EXISTS idx_reviews_domain ON reviews(domain);
CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(rating);
CREATE INDEX IF NOT EXISTS idx_reviews_date   ON reviews(review_date);
"""

# ── Підключення до PostgreSQL ────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(**PG_CONFIG)


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    log.info("✅ Таблиця reviews готова")

# ── Парсинг одного відгуку ───────────────────────────────────────────────────

def parse_date(s):
    """Спробує розпарсити різні формати дат від Apify."""
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d", "%B %d, %Y"):
        try:
            return datetime.strptime(s[:len(fmt)+2].strip(), fmt).date()
        except Exception:
            pass
    return None


def parse_review(item: dict, asin: str, domain: str) -> dict:
    """Нормалізує один запис від Apify."""
    attrs = item.get("productAttributes") or item.get("product_attributes") or ""
    if isinstance(attrs, dict):
        attrs = ", ".join(f"{k}: {v}" for k, v in attrs.items())

    images = item.get("images") or item.get("reviewImages") or []
    if isinstance(images, str):
        images = [images]

    return {
        "asin":               asin,
        "domain":             domain,
        "review_id":          item.get("reviewId") or item.get("id") or item.get("review_id"),
        "rating":             int(item["rating"]) if item.get("rating") is not None else None,
        "title":              (item.get("title") or item.get("reviewTitle") or "")[:500],
        "content":            item.get("text") or item.get("content") or item.get("reviewText") or "",
        "author":             item.get("reviewerName") or item.get("author") or "",
        "author_url":         item.get("reviewerUrl") or item.get("authorUrl") or "",
        "is_verified":        bool(item.get("isVerified") or item.get("is_verified") or False),
        "review_date":        parse_date(item.get("date") or item.get("reviewDate") or item.get("review_date")),
        "helpful_votes":      int(item.get("helpfulVotes") or item.get("helpful_votes") or 0),
        "product_attributes": attrs[:500] if attrs else None,
        "product_variant":    item.get("variant") or item.get("productVariant") or None,
        "images":             json.dumps(images) if images else None,
        "raw_json":           json.dumps(item),
    }

# ── Запис у PostgreSQL ───────────────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO reviews
    (asin, domain, review_id, rating, title, content, author, author_url,
     is_verified, review_date, helpful_votes, product_attributes,
     product_variant, images, raw_json)
VALUES %s
ON CONFLICT (asin, domain, review_id) DO UPDATE SET
    rating             = EXCLUDED.rating,
    title              = EXCLUDED.title,
    content            = EXCLUDED.content,
    helpful_votes      = EXCLUDED.helpful_votes,
    product_attributes = EXCLUDED.product_attributes,
    raw_json           = EXCLUDED.raw_json,
    scraped_at         = NOW()
"""

def upsert_reviews(conn, rows: list[dict]) -> int:
    if not rows:
        return 0
    values = [(
        r["asin"], r["domain"], r["review_id"], r["rating"],
        r["title"], r["content"], r["author"], r["author_url"],
        r["is_verified"], r["review_date"], r["helpful_votes"],
        r["product_attributes"], r["product_variant"],
        r["images"], r["raw_json"],
    ) for r in rows if r.get("review_id")]  # пропускаємо без ID

    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, values, page_size=500)
    conn.commit()
    return len(values)

# ── Apify: запуск актора ─────────────────────────────────────────────────────

def run_apify(asin: str, domain: str, max_reviews: int = 500) -> list[dict]:
    """
    Запускає junglee/amazon-reviews-scraper і повертає список відгуків.
    Документація актора: https://apify.com/junglee/amazon-reviews-scraper
    """
    client = ApifyClient(APIFY_TOKEN)

    run_input = {
        "productUrls": [{
            "url": f"https://www.{domain}/dp/{asin}"
        }],
        "maxReviews":       max_reviews,
        "includeImages":    True,
        "sortBy":           "recent",       # "recent" або "helpful"
        "filterByStar":     "all_stars",
    }

    log.info(f"▶ Запускаю Apify для ASIN={asin} domain={domain} max={max_reviews}")
    run = client.actor(ACTOR_ID).call(run_input=run_input)

    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    log.info(f"   ↳ отримано {len(items)} записів від Apify")
    return items

# ── Головна логіка ───────────────────────────────────────────────────────────

def etl_one(conn, asin: str, domain: str, max_reviews: int = 500):
    raw_items = run_apify(asin, domain, max_reviews)
    rows = [parse_review(item, asin, domain) for item in raw_items]
    saved = upsert_reviews(conn, rows)
    log.info(f"   ↳ збережено/оновлено {saved} відгуків у БД")
    return saved


def main():
    if not APIFY_TOKEN:
        raise ValueError("❌ APIFY_TOKEN не задано в .env")

    conn = get_conn()
    log.info(f"✅ Підключено до PostgreSQL: {PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']}")

    ensure_table(conn)

    total = 0
    for target in TARGETS:
        try:
            saved = etl_one(conn, target["asin"], target["domain"], max_reviews=500)
            total += saved
        except Exception as e:
            log.error(f"❌ Помилка для {target}: {e}")

    conn.close()
    log.info(f"🏁 Готово! Всього збережено: {total} відгуків")


if __name__ == "__main__":
    main()
