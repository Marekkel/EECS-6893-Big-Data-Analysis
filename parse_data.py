import pandas as pd
import numpy as np
import json


def parse_event_to_row(e: dict) -> dict:
    """Extract one flat row from a single Ticketmaster event JSON."""
    # priceRanges: 有的 event 根本没有这个字段，所以用 get + 默认 [{}]
    price = (e.get("priceRanges") or [{}])[0]

    # venue: 第一个场馆
    venue = e.get("_embedded", {}).get("venues", [{}])[0]

    # artist: 第一个 attraction
    attractions = e.get("_embedded", {}).get("attractions", [])
    artist = attractions[0] if attractions else {}

    # classification: 第一个分类
    classifications = e.get("classifications") or [{}]
    classification = classifications[0]

    row = {
        # Basic info
        "ID": e.get("id"),
        "Name": e.get("name"),
        "Date": e.get("dates", {}).get("start", {}).get("localDate"),

        # Artist & classification
        "Artist": artist.get("name"),
        "Segment": classification.get("segment", {}).get("name"),
        "Genre": classification.get("genre", {}).get("name"),
        "SubGenre": classification.get("subGenre", {}).get("name"),

        # Promoter
        "Promoter": e.get("promoter", {}).get("name"),

        # Price ranges (may be None if priceRanges missing)
        "Price_Ranges_Type": price.get("type"),
        "Price_Ranges_Currency": price.get("currency"),
        "Price_Ranges_Max": price.get("max"),
        "Price_Ranges_Min": price.get("min"),

        # Venue info
        "Venue": venue.get("name"),
        "Venue_City": venue.get("city", {}).get("name"),
        "Venue_State": venue.get("state", {}).get("stateCode"),
        "Venue_Country": venue.get("country", {}).get("name"),
        "Venue_Timezone": venue.get("timezone"),

        # Upcoming events counts
        "Upcoming_Events_Venue": venue.get("upcomingEvents", {}).get("_total"),
        "Upcoming_Events_Artist": artist.get("upcomingEvents", {}).get("_total"),
    }

    return row


rows = []
with open("./ticketmaster_raw/dt=2025-11-21/events_Phoenix.jsonl", "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        e = json.loads(line)
        rows.append(parse_event_to_row(e))

df = pd.DataFrame(rows)
df["Date_parsed"] = pd.to_datetime(df["Date"], errors="coerce")
df["Price_Ranges_Max"] = pd.to_numeric(df["Price_Ranges_Max"], errors="coerce")
df["Price_Ranges_Min"] = pd.to_numeric(df["Price_Ranges_Min"], errors="coerce")
df["Upcoming_Events_Venue"] = pd.to_numeric(df["Upcoming_Events_Venue"], errors="coerce")
df["Upcoming_Events_Artist"] = pd.to_numeric(df["Upcoming_Events_Artist"], errors="coerce")

print(df.head())
print(df.dtypes)