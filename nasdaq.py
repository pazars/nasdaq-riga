import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date, datetime
from pandas_gbq import to_gbq
from dotenv import load_dotenv


load_dotenv()

url = "https://nasdaqbaltic.com/statistics/lv/bonds"

column_name_map = {
    "Nosaukums": "name",
    "Kods": "code",
    "Kupons %": "coupon_rate",
    "Dzēšana": "end_date",
    "Nomināls": "face_value",
    "Uzkr.ienāk.": "unpaid_profit_pct",
    "Pied.": "clean_sell_price",
    "Piepr.": "clean_buy_price",
    "Pied._2": "dirty_sell_price",
    "Piepr._2": "dirty_buy_price",
    "Darījumi": "transaction_count",
    "Apgr. €": "turnover",
    "Emitēts": "issue_date",
    "Tirgus": "exchange_name",
    "Pēdējā tirdzniecības diena": "final_trade_date",
    "Pēdējā netīrā cena": "last_sold_dirty_price",
    "Pēdējā tīrā cena": "last_sold_clean_price",
}

try:
    print(f"Fetching website content from: {url}")
    # Use requests to get the HTML content of the page
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

    # Parse the HTML with BeautifulSoup
    soup = BeautifulSoup(response.content, "lxml")

    # Find the table element using its class name
    table = soup.find("table", class_="table-BABT_3")

    if not table:
        raise ValueError("Could not find the bonds table on the page.")

    # Extract headers and handle duplicates
    headers = []
    seen_headers = {}
    for th in table.find_all("th"):
        header = th.get_text(strip=True)
        if header in seen_headers:
            seen_headers[header] += 1
            headers.append(f"{header}_{seen_headers[header]}")
        else:
            seen_headers[header] = 1
            headers.append(header)

    # Extract rows (skip the header row explicitly)
    rows = []
    for tr in table.find_all("tr")[1:]:
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if cells and len(cells) == len(headers):
            rows.append(cells)

    # Build DataFrame with corrected headers
    df = pd.DataFrame(rows, columns=headers)

    print("Successfully fetched table content")

    # 4. Drop Pievienot column
    df = df.drop(columns=["Pievienot"])

    # Pied._2 dirty price, Pied. clean price

    # 3) Convert Uzkr.ienāk., Pied., Piepr. to floats (handle mixed dtypes safely)
    cols = [
        "Uzkr.ienāk.",
        "Pied.",
        "Pied._2",
        "Piepr.",
        "Piepr._2",
        "Pēdējā netīrā cena",
        "Pēdējā tīrā cena",
    ]
    for col in cols:
        df[col] = (
            df[col]
            .str.strip()  # Remove leading/trailing spaces
            .str.replace("^-", "", regex=True)  # Replace lone "-" with empty string
            .str.replace(" ", "", regex=False)  # Remove all spaces
            .str.replace(",", ".", regex=False)  # Replace decimal comma with a dot
        )

        # Finally, convert to numeric, coercing errors to NaN
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 1. Convert Dzēšana column to datetime
    for col in ["Dzēšana", "Emitēts", "Pēdējā tirdzniecības diena"]:
        df[col] = pd.to_datetime(df[col], format="%d.%m.%Y", errors="coerce")

    # 2. Clean Nomināls: remove EUR, spaces → int
    for col in ["Nomināls", "Apgr. €", "Kupons %"]:
        df[col] = pd.to_numeric(
            df[col]
            .str.replace(" EUR", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace(",", ".", regex=False)  # decimal separator
        )

except Exception as e:
    print(f"An error occurred: {e}")


# Refers to Google Sheets cells I had made before
A3 = df["Nomināls"]  # bond value
B3 = 7  # number of bonds to buy
C3 = df["Kupons %"]  # coupon rate
D3: pd.Series = df["Dzēšana"]  # bond expiry date
E3 = date.today()
F3 = df["Pied._2"]  # offer price, %
G3 = 0.2  # buy order commision, %
H3 = 0.01  # monthly interest on assets, %
I3 = 1  # monthly payment for account, EUR

K3 = A3 * (F3 / 100)  # Sell price, EUR
L3 = K3 * B3  # Total buy at sell price, EUR
M3 = (L3 * (G3 / 100)).clip(lower=20)  # trade commission, EUR
N3 = A3 * B3 * (C3 / 100)  # Yearly income, EUR
O3 = N3 / 12  # Monthly income, EUR
P3 = L3 * (H3 / 100) + I3  # Monthly payment for assets, EUR
Q3 = O3 - P3  # Income after payments
R3 = (L3 - (A3 * B3) + M3) / Q3  # number of months to break even
S3 = (D3.dt.year - E3.year) * 12 + D3.dt.month - E3.month  # months until bond expires
T3 = (S3 - R3) * Q3  # profit until end of bond
U3 = (
    T3 / (L3 + M3) / (S3 / 12) * 100
)  # actual yearly profit % incl. expenses,comissions
V3 = T3 * (1 - 0.255)  # profit after tax
W3 = V3 / (L3 + M3) / (S3 / 12) * 100  # actual yearly profit % incl. expenses after tax

df["months_left"] = S3
df["income_after_commisions"] = Q3
df["months_till_roi"] = R3
df["end_profit_before_tax"] = T3
df["end_profit_after_tax"] = V3
df["yearly_profit_pct_before_tax"] = U3
df["yearly_profit_pct_after_tax"] = W3

df["timestamp"] = datetime.now()

df = df.rename(columns=column_name_map)

print(df.head())

# Your BigQuery project ID and dataset.table name
project_id = os.getenv("GCP_PROJECT_ID")
table_id = os.getenv("BIG_QUERY_TABLE_ID")

# Append the DataFrame to the BigQuery table
to_gbq(df, table_id, project_id=project_id, if_exists="append")
