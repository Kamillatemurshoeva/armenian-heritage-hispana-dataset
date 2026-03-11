#!/usr/bin/env python3
-8)

import argparse
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests

BASE_URL = "https://api.europeana.eu/record/v2/search.json"
HISPANA_FILTER = 'PROVIDER:"Hispana"'
ROWS = 100


def _join(val: Any) -> str:
    if isinstance(val, list):
        return "; ".join(str(v) for v in val if v)
    return str(val) if val else ""


def _get_date(item: Dict[str, Any]) -> str:
    for key in ("year", "dcDate", "edmTimespanLabel"):
        val = item.get(key)
        if val:
            return _join(val)
    return ""


def fetch_all(api_key: str, query: str, sleep_s: float = 0.8) -> pd.DataFrame:
    start = 1
    out: List[Dict[str, Any]] = []

    while True:
        params = {
            "wskey": api_key,
            "query": query,
            "qf": HISPANA_FILTER,
            "rows": ROWS,
            "start": start,
        }
        print(f"Fetching start={start}...", file=sys.stderr)
        r = requests.get(BASE_URL, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()

        items = data.get("items", [])
        if not items:
            break

        for item in items:
            out.append(
                {
                    "title": _join(item.get("title")),
                    "creator": _join(item.get("dcCreator")),
                    "date": _get_date(item),
                    "description": _join(item.get("dcDescription")),
                    "original_url": _join(item.get("edmIsShownAt")),
                    "europeana_link": item.get("guid") or "",
                    "id": item.get("id") or "",
                    "type": item.get("type") or "",
                    "provider": _join(item.get("provider")),
                    "data_provider": _join(item.get("dataProvider")),
                    "country": _join(item.get("country")),
                    "language": _join(item.get("language")),
                }
            )

        if len(items) < ROWS:
            break

        start += ROWS
        time.sleep(sleep_s)

    return pd.DataFrame(out)


def fetch_many(api_key: str, query: str, sleep_s: float = 0.8) -> pd.DataFrame:
    if "," not in query:
        return fetch_all(api_key, query.strip(), sleep_s=sleep_s)

    terms = [q.strip() for q in query.split(",") if q.strip()]
    if not terms:
        return fetch_all(api_key, query.strip(), sleep_s=sleep_s)

    frames = []
    for term in terms:
        print(f"=== Fetching for term: {term!r} ===", file=sys.stderr)
        df_term = fetch_all(api_key, term, sleep_s=sleep_s)
        if not df_term.empty:
            df_term["search_term"] = term
        frames.append(df_term)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    if "id" in df.columns and "original_url" in df.columns:
        df = df.sort_values(["id", "original_url"]).drop_duplicates(subset=["id"], keep="first")
    if "original_url" in df.columns:
        df = df.drop_duplicates(subset=["original_url"], keep="first")

    return df.reset_index(drop=True)


def clean(df: pd.DataFrame, filter_armenian: bool = True) -> pd.DataFrame:
    cols = [
        "title",
        "date",
        "creator",
        "description",
        "original_url",
        "europeana_link",
        "id",
        "type",
        "provider",
        "data_provider",
        "country",
        "language",
        "armenian_match_field",
        "armenian_relevance",
    ]
    if df.empty:
        return pd.DataFrame(columns=cols)

    df = df.fillna("").copy()
    for col in df.columns:
        df[col] = df[col].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    if filter_armenian:
        pat = re.compile(r"\barmeni\w*\b", re.IGNORECASE)

        title_has = df["title"].str.contains(pat, na=False, regex=True)
        desc_has = df["description"].str.contains(pat, na=False, regex=True)

        pub_noise_pat = re.compile(
            r"\b(?:editorial|ediciones|imprenta|imp\.?|ed\.)\s+armeni\w*\b",
            re.IGNORECASE,
        )
        desc_pub_only = df["description"].str.contains(pub_noise_pat, na=False, regex=True) & ~title_has

        mask = (title_has | desc_has) & ~desc_pub_only
        df = df[mask].copy()

        match_field = []
        relevance = []
        for title, desc in zip(df["title"], df["description"]):
            t = bool(pat.search(title))
            d = bool(pat.search(desc))

            if t and d:
                match_field.append("title+description")
            elif t:
                match_field.append("title")
            else:
                match_field.append("description")

            desc_low = desc.lower()
            idx = desc_low.find("armeni")
            early_hit = 0 <= idx <= 200

            relevance.append("direct" if (t or early_hit) else "incidental")

        df["armenian_match_field"] = match_field
        df["armenian_relevance"] = relevance
    else:
        df["armenian_match_field"] = ""
        df["armenian_relevance"] = ""

    df = df.sort_values(["id", "original_url"]).drop_duplicates(subset=["id"], keep="first")
    df = df.drop_duplicates(subset=["original_url"], keep="first")
    return df[cols].reset_index(drop=True)


def save_outputs(df_raw: pd.DataFrame, df_clean: pd.DataFrame, out_dir: Path, prefix: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_csv = out_dir / f"{prefix}_raw.csv"
    clean_csv = out_dir / f"{prefix}_clean.csv"
    clean_jsonl = out_dir / f"{prefix}_clean.jsonl"

    df_raw.to_csv(raw_csv, index=False, encoding="utf-8")
    df_clean.to_csv(clean_csv, index=False, encoding="utf-8")
    df_clean.to_json(clean_jsonl, orient="records", lines=True, force_ascii=False)

    print("\nSaved files:", file=sys.stderr)
    print(f"  RAW CSV:   {raw_csv.resolve()}", file=sys.stderr)
    print(f"  CLEAN CSV: {clean_csv.resolve()}", file=sys.stderr)
    print(f"  JSONL:     {clean_jsonl.resolve()}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract Hispana metadata via Europeana API")
    parser.add_argument(
        "-q",
        "--query",
        default="armenio, armenia, armenios, armenias",
        help='Search query or comma-separated list. Default: "armenio, armenia, armenios, armenias"',
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default="data",
        help="Output directory (default: data)",
    )
    parser.add_argument(
        "--no-filter",
        action="store_true",
        help="Do not filter by armeni* in title/description",
    )
    parser.add_argument(
        "--only-direct",
        action="store_true",
        help="Keep only records marked as directly related (heuristic)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.8,
        help="Delay between requests in seconds (default: 0.8)",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("EUROPEANA_API_KEY", ""),
        help="Europeana API key (or set env EUROPEANA_API_KEY)",
    )
    args = parser.parse_args()

    if not args.api_key:
        print("ERROR: Missing Europeana API key. Set EUROPEANA_API_KEY or use --api-key.", file=sys.stderr)
        sys.exit(2)

    df_raw = fetch_many(args.api_key, args.query, sleep_s=args.sleep)
    print(f"Raw collected: {len(df_raw)}", file=sys.stderr)

    df_clean = clean(df_raw, filter_armenian=not args.no_filter)

    if args.only_direct and not df_clean.empty and "armenian_relevance" in df_clean.columns:
        df_clean = df_clean[df_clean["armenian_relevance"] == "direct"].reset_index(drop=True)

    print(f"Cleaned: {len(df_clean)}", file=sys.stderr)

    first_term = args.query.split(",")[0].strip() if args.query else ""
    q = first_term.replace(" ", "_")[:15]
    prefix = f"hispana_europeana_{q}" if q else "hispana_europeana"

    save_outputs(df_raw, df_clean, Path(args.output_dir), prefix)


if __name__ == "__main__":
    main()
