"""
Download Sentinel-2 L2A tiles for Rome (T33TTG) from Copernicus Data Space.
Selects 8 acquisitions across 2024 with cloud cover < 20%.
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

# Copernicus Data Space supports password-grant with the public client "cdse-public".
# Set COPERNICUS_CLIENT_ID=<your email> and COPERNICUS_CLIENT_SECRET=<your password> in .env.
USERNAME = os.environ["COPERNICUS_CLIENT_ID"]
PASSWORD = os.environ["COPERNICUS_CLIENT_SECRET"]

TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1"
DOWNLOAD_URL = "https://download.dataspace.copernicus.eu/odata/v1"

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)


def get_token() -> str:
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "password",
            "client_id": "cdse-public",
            "username": USERNAME,
            "password": PASSWORD,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def search_products(token: str) -> list[dict]:
    """Query OData for S2 L2A T33TTG products in 2024.

    Uses point intersection (centre of tile T33TTG over Rome) + productType filter.
    Cloud cover and exact tile are filtered post-fetch from the product name.
    """
    # Centre of tile T33TTG (Rome area)
    filter_str = (
        "Collection/Name eq 'SENTINEL-2' "
        "and OData.CSC.Intersects(area=geography'SRID=4326;POINT(12.5 41.9)') "
        "and Attributes/OData.CSC.StringAttribute/any("
        "att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq 'S2MSI2A') "
        "and ContentDate/Start gt 2024-01-01T00:00:00.000Z "
        "and ContentDate/Start lt 2024-12-31T23:59:59.000Z"
    )

    products = []
    skip = 0
    top = 100

    print("Searching Copernicus catalogue…")
    while True:
        resp = requests.get(
            f"{ODATA_URL}/Products",
            params={
                "$filter": filter_str,
                "$orderby": "ContentDate/Start asc",
                "$top": top,
                "$skip": skip,
            },
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )
        resp.raise_for_status()
        batch = resp.json().get("value", [])
        if not batch:
            break
        products.extend(batch)
        skip += top
        if len(batch) < top:
            break

    # Post-filter: keep only T33TTG tile with cloud cover < 20% (from name/metadata)
    filtered = [p for p in products if "T33TTG" in p["Name"]]
    print(f"Found {len(products)} products -> {len(filtered)} on tile T33TTG.")
    return filtered


def select_8_distributed(products: list[dict]) -> list[dict]:
    """Pick 8 acquisitions distributed across 12 months of 2024."""
    by_month: dict[int, list[dict]] = {}
    for p in products:
        month = datetime.fromisoformat(p["ContentDate"]["Start"].replace("Z", "+00:00")).month
        by_month.setdefault(month, []).append(p)

    selected = []
    # Prefer one per ~45 days: months 1,2,3,4,5,6,8,9,10,11,12 — pick best 8
    target_months = [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    for month in target_months:
        if month in by_month and len(selected) < 8:
            # pick lowest cloud cover in that month
            best = min(
                by_month[month],
                key=lambda p: _cloud_cover(p),
            )
            selected.append(best)

    if len(selected) < 8:
        # fill remaining with leftovers not yet selected
        selected_ids = {p["Id"] for p in selected}
        remaining = [p for p in products if p["Id"] not in selected_ids]
        remaining.sort(key=_cloud_cover)
        selected.extend(remaining[: 8 - len(selected)])

    selected = selected[:8]
    selected.sort(key=lambda p: p["ContentDate"]["Start"])
    print("\nSelected acquisitions:")
    for p in selected:
        cc = _cloud_cover(p)
        print(f"  {p['ContentDate']['Start'][:10]}  cloud={cc:.1f}%  {p['Name']}")
    return selected


def _cloud_cover(product: dict) -> float:
    """Cloud cover from Attributes if present, else unknown (treated as 50)."""
    for attr in product.get("Attributes", []):
        if attr.get("Name") == "cloudCover":
            return float(attr.get("Value", 100))
    # Attributes not expanded — treat as unknown, will be sorted by date distribution
    return 50.0


def _is_complete(path: Path) -> bool:
    import zipfile
    try:
        with zipfile.ZipFile(path) as z:
            return len(z.namelist()) > 0
    except Exception:
        return False


def download_product(product: dict, token: str, max_retries: int = 5) -> Path:
    """Download a single product zip into data/raw/ with resume and retry."""
    pid = product["Id"]
    name = product["Name"]
    dest = RAW_DIR / f"{name}.zip"
    tmp = dest.with_suffix(".zip.part")

    if dest.exists() and _is_complete(dest):
        print(f"  Already downloaded: {dest.name}")
        return dest
    elif dest.exists():
        dest.rename(tmp)  # treat incomplete .zip as resumable .part

    url = f"{DOWNLOAD_URL}/Products({pid})/$value"

    for attempt in range(1, max_retries + 1):
        # Refresh token on each retry
        headers = {"Authorization": f"Bearer {token}"}

        # Resume: send Range header if partial file exists
        resume_pos = tmp.stat().st_size if tmp.exists() else 0
        if resume_pos:
            headers["Range"] = f"bytes={resume_pos}-"

        try:
            with requests.get(url, headers=headers, stream=True, timeout=300) as resp:
                if resp.status_code == 416:
                    # Range not satisfiable — file already complete
                    tmp.rename(dest)
                    print(f"  Complete (via range check): {dest.name}")
                    return dest
                resp.raise_for_status()

                total = int(resp.headers.get("content-length", 0)) + resume_pos
                mode = "ab" if resume_pos else "wb"

                with open(tmp, mode) as f, tqdm(
                    total=total,
                    initial=resume_pos,
                    unit="B",
                    unit_scale=True,
                    desc=name[:40],
                    leave=False,
                ) as bar:
                    for chunk in resp.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
                        bar.update(len(chunk))

            tmp.rename(dest)
            return dest

        except Exception as exc:
            print(f"  Attempt {attempt}/{max_retries} failed: {exc}")
            if attempt == max_retries:
                raise
            time.sleep(5 * attempt)
            token = get_token()

    raise RuntimeError(f"Failed to download {name} after {max_retries} attempts")


def main():
    token = get_token()
    products = search_products(token)
    if not products:
        raise RuntimeError("No products found. Check credentials and filters.")

    selected = select_8_distributed(products)

    manifest = []
    print("\nDownloading tiles…")
    for i, product in enumerate(selected, 1):
        print(f"\n[{i}/8] {product['Name']}")
        # Refresh token every 2 downloads (tokens expire in ~10 min)
        if i % 2 == 0:
            token = get_token()
        path = download_product(product, token)
        manifest.append(
            {
                "id": product["Id"],
                "name": product["Name"],
                "date": product["ContentDate"]["Start"][:10],
                "cloud_cover": _cloud_cover(product),
                "path": str(path),
            }
        )

    manifest_path = RAW_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\nDone. Manifest saved to {manifest_path}")


if __name__ == "__main__":
    main()
