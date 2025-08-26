#!/usr/bin/env python3
import csv
from pathlib import Path
from collections import Counter

def collect_pages_from_file(csv_path: Path) -> list[int]:
    """Read the 'page' column from a CSV file."""
    pages = []
    with csv_path.open("r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        if "page" not in reader.fieldnames:
            return pages
        for row in reader:
            try:
                pages.append(int(row["page"]))
            except (ValueError, TypeError):
                continue
    return pages

def main():
    base = Path(".").resolve()  # current directory (run inside /Scraper)
    files = sorted(base.glob("failed_pages*.csv"))
    if not files:
        print(f"❌ No files found in {base} matching failed_pages*.csv")
        return

    all_pages = []
    for f in files:
        pages = collect_pages_from_file(f)
        all_pages.extend(pages)
        print(f"• {f.name}: {len(pages)} pages")

    # Final list
    print("\n✅ Final list of failed pages:")
    print(all_pages)

    # Count
    print("\n📊 Total pages in list:", len(all_pages))

    # Duplicate checker
    counts = Counter(all_pages)
    duplicates = {p: c for p, c in counts.items() if c > 1}
    print("\n📊 Total unique pages:", len(counts))
    if duplicates:
        print(f"⚠️ Duplicates found ({len(duplicates)} unique pages are duplicated):")
        for p in sorted(duplicates):
            print(f"  page {p}: {duplicates[p]} times")
    else:
        print("✅ No duplicates found.")

if __name__ == "__main__":
    main()
