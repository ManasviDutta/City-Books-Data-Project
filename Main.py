import requests
from bs4 import BeautifulSoup
import pandas as pd

# Step 1: Scrape NYT Hardcover Nonfiction Bestsellers
url = "https://www.nytimes.com/books/best-sellers/hardcover-nonfiction/"
response = requests.get(url)
soup = BeautifulSoup(response.content, "html.parser")

book_data = []

# Each book listing is within a <li> inside the ol[data-testid='topic-list']
books_list = soup.find("ol", {"data-testid": "topic-list"})

if books_list:
    books = books_list.find_all("li")

    for book in books:
        try:
            title = book.find("h3").get_text(strip=True)
            author_tag = book.find("p", {"itemprop": "author"})
            author = author_tag.get_text(strip=True) if author_tag else "Unknown"
            book_data.append({"Title": title, "Author": author})
        except Exception as e:
            print("Skipping book due to error:", e)

    # Convert to DataFrame
    df1 = pd.DataFrame(book_data)
    print("✅ Scraped NYT Bestsellers:\n", df1.head())
else:
    print("Could not find the books list. NYT page structure may have changed.")


    '''second part '''

import time

API_KEY = 'AIzaSyAAa_k6l2axINW3ALPDYoIMXB7pC8snm94'
google_api_url = "https://www.googleapis.com/books/v1/volumes"

book_info = []

for title in df1["Title"]:
    query = title.replace(" ", "+")
    url = f"{google_api_url}?q={query}&key={API_KEY}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        items = data.get("items", [])
        if items:
            volume_info = items[0].get("volumeInfo", {})
            book_info.append({
                "Title": title,
                "PublishedDate": volume_info.get("publishedDate", "N/A"),
                "PageCount": volume_info.get("pageCount", "N/A"),
                "Categories": ", ".join(volume_info.get("categories", ["N/A"])),
                "AverageRating": volume_info.get("averageRating", "N/A"),
                "Language": volume_info.get("language", "N/A")
            })
        else:
            print(f" No data for: {title}")
            book_info.append({
                "Title": title,
                "PublishedDate": "N/A",
                "PageCount": "N/A",
                "Categories": "N/A",
                "AverageRating": "N/A",
                "Language": "N/A"
            })
    else:
        print(f" API call failed for: {title}")
        book_info.append({
            "Title": title,
            "PublishedDate": "N/A",
            "PageCount": "N/A",
            "Categories": "N/A",
            "AverageRating": "N/A",
            "Language": "N/A"
        })

    time.sleep(1)  # avoid hitting rate limits

# Create DF2
df2 = pd.DataFrame(book_info)
print("\n Google Books API Data:")
print(df2.head())


'''create final dataframe'''

# STEP 3: MERGE NYT (DF1) AND GOOGLE BOOKS (DF2)
df3 = pd.merge(df1, df2, on="Title")
print("Final Combined Data:")
print(df3.head())

'''step 4 save to csv'''

# STEP 4: SAVE TO CSV
df3.to_csv("nyt_books_google_data.csv", index=False)

# STEP 5: SAVE TO SQLITE DATABASE
from sqlalchemy import create_engine

engine = create_engine("sqlite:///nyt_books.db")
df3.to_sql("books", engine, if_exists="replace", index=False)

# STEP 6: PRINT SUMMARY STATS
print("Summary Statistics:")
print(df3.describe(include='all'))
