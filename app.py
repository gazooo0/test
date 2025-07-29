import streamlit as st
import pandas as pd
import unicodedata
import re
import time
import os
import json
from bs4 import BeautifulSoup
import requests
import gspread
from google.oauth2.service_account import Credentials

# === 設定 ===
HEADERS = {"User-Agent": "Mozilla/5.0"}
CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# === Google Sheets設定 ===
SHEET_ID = "1wMkpbOvqveVBkJSR85mpZcnKThYSEmusmsl710SaRKw"
SHEET_NAME = "cache"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Render用：環境変数から認証情報を読み込む
if "GOOGLE_SERVICE_JSON" in os.environ:
    service_account_info = json.loads(os.environ["GOOGLE_SERVICE_JSON"])
else:
    with open("service_account.json", "r", encoding="utf-8") as f:
        service_account_info = json.load(f)

credentials = Credentials.from_service_account_info(service_account_info, scopes=SCOPE)
gc = gspread.authorize(credentials)
sheet = gc.open_by_key(SHEET_ID).worksheet(SHEET_NAME)

# === ウマ娘血統データの読み込み ===
umamusume_df = pd.read_csv("umamusume.csv")
image_dict = dict(zip(umamusume_df["kettou"], umamusume_df["url"]))
umamusume_bloodlines = set(umamusume_df["kettou"].dropna().astype(str))
normalized_umamusume = {unicodedata.normalize("NFKC", n).strip().lower() for n in umamusume_bloodlines}

# === 血統位置ラベル ===
def generate_position_labels():
    def dfs(pos, depth, max_depth):
        if depth > max_depth: return []
        result = [pos]
        result += dfs(pos + "父", depth + 1, max_depth)
        result += dfs(pos + "母", depth + 1, max_depth)
        return result
    return dfs("", 0, 5)[1:]
POSITION_LABELS = generate_position_labels()

# === 出走馬リンク取得 ===
def get_horse_links(race_id):
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    res = requests.get(url, headers=HEADERS)
    res.encoding = "EUC-JP"
    soup = BeautifulSoup(res.text, "html.parser")
    horse_links = {}
    tables = soup.find_all("table", class_="RaceTable01")
    for table in tables:
        for a in table.find_all("a", href=True):
            if "/horse/" in a["href"]:
                name = a.get_text(strip=True)
                full_url = "https://db.netkeiba.com" + a["href"]
                if len(name) >= 2 and name not in horse_links:
                    horse_links[name] = full_url
    return horse_links

# === 血統取得 ===
def get_pedigree_with_positions(horse_url):
    horse_id = horse_url.rstrip("/").split("/")[-1]
    ped_url = f"https://db.netkeiba.com/horse/ped/{horse_id}/"
    res = requests.get(ped_url, headers=HEADERS)
    res.encoding = "EUC-JP"
    soup = BeautifulSoup(res.text, "html.parser")
    table = soup.find("table", class_="blood_table")
    if not table:
        return {}
    names = {}
    td_list = table.find_all("td")
    for i, td in enumerate(td_list[:len(POSITION_LABELS)]):
        label = POSITION_LABELS[i]
        a = td.find("a")
        if a and a.text.strip():
            names[label] = a.text.strip()
    return names

# === 照合処理 ===
def match_umamusume(pedigree_dict):
    matched = []
    for pos, name in pedigree_dict.items():
        key = unicodedata.normalize("NFKC", name).strip().lower()
        if key in normalized_umamusume:
            img_url = image_dict.get(name, "")
            if img_url:
                matched.append(
                    f"<img src='{img_url}' width='100' style='vertical-align:middle;margin-right:8px;'>【{pos}】{name}"
                )
            else:
                matched.append(f"【{pos}】{name}")
    return matched

# === キャッシュ処理 ===
def get_cache_filename(race_id):
    return os.path.join(CACHE_DIR, f"{race_id}.csv")

def load_cached_result(race_id):
    records = sheet.get_all_records()
    matched = [r for r in records if r["race_id"] == race_id]
    if matched:
        return pd.DataFrame(matched)
    filepath = get_cache_filename(race_id)
    if os.path.exists(filepath):
        return pd.read_csv(filepath)
    return None

def save_cached_result(race_id, df):
    df["race_id"] = race_id
    sheet.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")
    df.to_csv(get_cache_filename(race_id), index=False)

# === UI ===
st.title("ウマ娘血統🐎サーチ")

schedule_df = pd.read_csv("jra_2025_keibabook_schedule.csv")
schedule_df["日付"] = pd.to_datetime(
    schedule_df["年"].astype(str) + "/" + schedule_df["月日(曜日)"].str.extract(r"(\d{2}/\d{2})")[0],
    format="%Y/%m/%d"
)

today = pd.Timestamp.today()
past_31 = today - pd.Timedelta(days=31)
future_7 = today + pd.Timedelta(days=7)
schedule_df = schedule_df[schedule_df["日付"].between(past_31, future_7)]

dates = sorted(schedule_df["日付"].dt.strftime("%Y-%m-%d").unique(), reverse=True)
st.markdown("### 📅 競馬開催日を選択")
selected_date = st.selectbox("（直近30日前後の開催まで遡って表示できます。）", dates)
data_filtered = schedule_df[schedule_df["日付"].dt.strftime("%Y-%m-%d") == selected_date]

st.markdown("### 🏟 競馬場を選択")
place_codes = {"札幌": "01", "函館": "02", "福島": "03", "新潟": "04", "東京": "05",
               "中山": "06", "中京": "07", "京都": "08", "阪神": "09", "小倉": "10"}
available_places = sorted(data_filtered["競馬場"].unique())
cols = st.columns(5)
if "place" not in st.session_state:
    st.session_state.place = None
for i, p in enumerate(available_places):
    if cols[i % 5].button(p):
        st.session_state.place = p
place = st.session_state.place
if not place:
    st.stop()

st.markdown("### 🏁 レース番号を選択")
race_num_int = st.selectbox("レース番号を選んでください", list(range(1, 13)), format_func=lambda x: f"{x}R")
if not race_num_int:
    st.stop()

filtered = data_filtered[data_filtered["競馬場"] == place]
if filtered.empty:
    st.warning(f"⚠ {place} 競馬のレース情報が見つかりませんでした。")
    st.stop()

selected_row = filtered.iloc[0]
jj = place_codes.get(place, "")
kk = f"{int(selected_row['開催回']):02d}"
dd = f"{int(selected_row['日目']):02d}"
race_id = f"{selected_row['年']}{jj}{kk}{dd}{race_num_int:02d}"
st.markdown(f"**race_id**: {race_id}")

use_cache = st.checkbox("キャッシュが存在する場合は再利用する", value=True)

if st.button("🔍 ウマ娘血統の馬サーチを開始"):
    cached_df = load_cached_result(race_id) if use_cache else None

    if cached_df is not None:
        st.success(f"✅ キャッシュから {len(cached_df)}頭を表示")
        for idx, row in cached_df.iterrows():
            st.markdown(f"""
<div style='font-size:20px; font-weight:bold;'>{idx + 1}. {row["馬名"]}</div>
該当血統数：{row["該当数"]}<br>
{row["該当箇所"]}
""", unsafe_allow_html=True)
            st.markdown("---")
    else:
        horse_links = get_horse_links(race_id)
        st.markdown(f"🏇 出走馬数: {len(horse_links)}頭")
        result_rows = []
        for idx, (name, link) in enumerate(horse_links.items(), 1):
            with st.spinner(f"{idx}頭目：{name} を照合中..."):
                try:
                    pedigree = get_pedigree_with_positions(link)
                    matches = match_umamusume(pedigree)
                    st.markdown(f"""
<div style='font-size:20px; font-weight:bold;'>{idx}. {name}</div>
該当血統数：{len(matches)}<br>
{ "<br>".join(matches) if matches else "該当なし" }
""", unsafe_allow_html=True)
                    result_rows.append({
                        "馬名": name,
                        "該当数": len(matches),
                        "該当箇所": "<br>".join(matches) if matches else "該当なし"
                    })
                except Exception as e:
                    st.error(f"{name} の照合中にエラーが発生しました：{e}")
            st.markdown("---")
            time.sleep(1.2)
        if result_rows:
            df = pd.DataFrame(result_rows)
            save_cached_result(race_id, df)
