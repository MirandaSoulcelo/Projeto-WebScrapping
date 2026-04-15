"""
Web Scraping — Books to Scrape (books.toscrape.com)
v2 — com retry automático e salvamento de progresso parcial
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import os
import json

BASE_URL = "https://books.toscrape.com/catalogue/"
START_URL = "https://books.toscrape.com/catalogue/page-1.html"
CHECKPOINT_FILE = "books_checkpoint.json"

RATING_MAP = {
    "One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5
}

def get_soup(url, retries=5, wait=3):
    """ requisição HTTP com retry automático em caso de timeout."""
    headers = {"User-Agent": "Mozilla/5.0 (educational scraper)"}
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            print(f"      Tentativa {attempt}/{retries} falhou: {type(e).__name__}")
            if attempt < retries:
                espera = wait * attempt
                print(f"     Aguardando {espera}s...")
                time.sleep(espera)
            else:
                print(f"     Pulando URL após {retries} tentativas: {url}")
                return None

def save_checkpoint(books, page):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump({"page": page, "books": books}, f, ensure_ascii=False)



def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f" Checkpoint encontrado! Retomando da página {data['page']} com {len(data['books'])} livros já coletados.")
        return data["books"], data["page"]
    return [], 1



def scrape_all_books():
    books, start_page = load_checkpoint()
    already_collected_upcs = {b["upc"] for b in books}

    url = START_URL if start_page == 1 else BASE_URL + f"page-{start_page}.html"
    page = start_page

    while url:
        print(f"  Coletando página {page}...")
        soup = get_soup(url)

        if soup is None:
            page += 1
            url = BASE_URL + f"page-{page}.html" if page <= 50 else None
            continue

        for article in soup.find_all("article", class_="product_pod"):
            title = article.find("h3").find("a")["title"]
            price_text = article.find("p", class_="price_color").text.strip()
            price = float(re.sub(r"[^\d.]", "", price_text))
            rating_class = article.find("p", class_="star-rating")["class"][1]
            rating = RATING_MAP.get(rating_class, 0)
            in_stock_tag = article.find("p", class_="instock")
            in_stock = 1 if in_stock_tag and "In stock" in in_stock_tag.text else 0
            relative_url = article.find("h3").find("a")["href"]
            book_url = BASE_URL + relative_url.replace("../", "")

            detail_soup = get_soup(book_url)
            if detail_soup is None:
                continue

            table = detail_soup.find("table", class_="table-striped")
            info = {}
            if table:
                for row in table.find_all("tr"):
                    info[row.find("th").text.strip()] = row.find("td").text.strip()

            upc = info.get("UPC", None)
            if upc in already_collected_upcs:
                continue
            already_collected_upcs.add(upc)

            breadcrumb = detail_soup.find("ul", class_="breadcrumb")
            category = "Unknown"
            if breadcrumb:
                crumbs = breadcrumb.find_all("li")
                if len(crumbs) >= 3:
                    category = crumbs[2].text.strip()

            price_excl = float(re.sub(r"[^\d.]", "", info.get("Price (excl. tax)", ""))) if info.get("Price (excl. tax)") else price
            price_incl = float(re.sub(r"[^\d.]", "", info.get("Price (incl. tax)", ""))) if info.get("Price (incl. tax)") else price
            stock_match = re.search(r"\d+", info.get("Availability", ""))
            stock_qty = int(stock_match.group()) if stock_match else (1 if in_stock else 0)

            books.append({
                "title": title,
                "category": category,
                "rating": rating,
                "price_gbp": price_excl,
                "price_incl_tax": price_incl,
                "stock_quantity": stock_qty,
                "in_stock": "Yes" if in_stock else "No",
                "upc": upc,
            })

            time.sleep(0.1)

        save_checkpoint(books, page + 1)
        print(f"     {len(books)} livros coletados até agora")

        next_btn = soup.find("li", class_="next")
        if next_btn:
            url = BASE_URL + next_btn.find("a")["href"]
            page += 1
        else:
            url = None

    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

    return books


print("=" * 50)
print("Iniciando scraping — Books to Scrape v2")
print("=" * 50)

books_data = scrape_all_books()
df_raw = pd.DataFrame(books_data)

print(f"\n Total de registros coletados: {len(df_raw)}")
print(df_raw.head(3).to_string())

df_raw.to_csv("books_raw.csv", index=False, encoding="utf-8")
print("\n Salvo: books_raw.csv")