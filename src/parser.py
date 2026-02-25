import html as html_lib
import json
import re
from typing import Any, Iterable

PINIA_MARKER = "window.__PINIA__"
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.IGNORECASE | re.DOTALL)
META_DESC_RE = re.compile(
    r'<meta\s+name="description"\s+content="(.*?)"', re.IGNORECASE | re.DOTALL
)
OG_IMAGE_RE = re.compile(
    r'<meta\s+property="og:image"\s+content="(.*?)"', re.IGNORECASE | re.DOTALL
)
CAROUSEL_COUNT_RE = re.compile(r"Item\s+\d+\s+of\s+(\d+)", re.IGNORECASE)
PLATE_RE = re.compile(r"\b[А-ЯІЇЄA-Z]{2}\d{4}[А-ЯІЇЄA-Z]{2}\b")
PRICE_RE = re.compile(r"\b(\d[\d\s]{1,9})\s*\$")
VIN_RE = re.compile(r"\b[A-HJ-NPR-Z0-9]{17}\b")
ODOMETER_THS_RE = re.compile(r"пробіг\s+([\d\s]+)\s*тис\.?\s*км", re.IGNORECASE)
ODOMETER_KM_RE = re.compile(r"пробіг\s+([\d\s]+)\s*км", re.IGNORECASE)
SELLER_RE = re.compile(r"продав(ець|ец)\s+([^,]+)", re.IGNORECASE)


def _find_json_end(text: str, start: int) -> int | None:
    """Return the end index of a JSON object starting at `start`."""
    depth = 0
    in_string = False
    escape = False
    for idx in range(start, len(text)):
        ch = text[idx]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return idx + 1
    return None


def extract_pinia_payload(html: str) -> dict[str, Any] | None:
    """Extract window.__PINIA__ JSON payload from HTML."""
    idx = html.find(PINIA_MARKER)
    if idx == -1:
        return None
    start = html.find("{", idx)
    if start == -1:
        return None
    end = _find_json_end(html, start)
    if end is None:
        return None
    try:
        return json.loads(html[start:end])
    except json.JSONDecodeError:
        return None


def _deep_find(data: Any, key: str) -> Iterable[Any]:
    """Yield all values for a key in nested dict/list structures."""
    if isinstance(data, dict):
        for k, v in data.items():
            if k == key:
                yield v
            yield from _deep_find(v, key)
    elif isinstance(data, list):
        for item in data:
            yield from _deep_find(item, key)


def _first_str(values: Iterable[Any]) -> str | None:
    """Return the first non-empty string from an iterable."""
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _first_int(values: Iterable[Any]) -> int | None:
    """Return the first int-like value from an iterable."""
    for value in values:
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            digits = re.sub(r"\D", "", value)
            if digits:
                return int(digits)
    return None


def _as_dict(value: Any) -> dict[str, Any]:
    """Return dict value or an empty dict."""
    return value if isinstance(value, dict) else {}


def _parse_title(html_text: str) -> str:
    """Extract <title> text from HTML."""
    match = TITLE_RE.search(html_text)
    return html_lib.unescape(match.group(1)).strip() if match else ""


def _parse_meta_description(html_text: str) -> str:
    """Extract meta description content from HTML."""
    match = META_DESC_RE.search(html_text)
    return html_lib.unescape(match.group(1)).strip() if match else ""


def _parse_og_image(html_text: str) -> str:
    """Extract og:image content from HTML."""
    match = OG_IMAGE_RE.search(html_text)
    return html_lib.unescape(match.group(1)).strip() if match else ""


def _parse_price_from_title(title: str) -> str | None:
    """Extract USD price from title-like text."""
    match = PRICE_RE.search(title)
    if not match:
        return None
    return re.sub(r"\s+", "", match.group(1))


def _parse_vin_from_title(title: str) -> str | None:
    """Extract VIN from title-like text."""
    match = VIN_RE.search(title)
    return match.group(0) if match else None


def _parse_plate_from_title(title: str) -> str | None:
    """Extract license plate from title-like text."""
    match = PLATE_RE.search(title)
    return match.group(0) if match else None


def _parse_odometer_from_description(description: str) -> int | None:
    """Extract odometer in km from description text."""
    match = ODOMETER_THS_RE.search(description)
    if match:
        digits = re.sub(r"\D", "", match.group(1))
        return int(digits) * 1000 if digits else None
    match = ODOMETER_KM_RE.search(description)
    if match:
        digits = re.sub(r"\D", "", match.group(1))
        return int(digits) if digits else None
    return None


def _parse_price_from_description(description: str) -> str | None:
    """Extract price from description text."""
    return _parse_price_from_title(description)


def _parse_username_from_description(description: str) -> str | None:
    """Extract seller name from description text."""
    match = SELLER_RE.search(description)
    if match:
        value = match.group(2).strip()
        value = value.split(" на ")[0].strip()
        return value
    return None


def parse_listing(
    html: str, url: str
) -> tuple[dict[str, Any], dict[str, Any] | None, list[str]]:
    """Parse listing fields and phone metadata from HTML."""
    title_html = _parse_title(html)
    meta_description = _parse_meta_description(html)
    og_image = _parse_og_image(html)
    pinia = extract_pinia_payload(html) or {}
    page = _as_dict(pinia.get("page"))
    structures = _as_dict(page.get("structures"))
    root = next(iter(structures.values()), {}) if structures else {}

    additional = _as_dict(_as_dict(root).get("additionalParams"))
    ld_json: Any = _as_dict(_as_dict(root).get("ldJSON"))
    if isinstance(ld_json, str):
        try:
            ld_json = json.loads(ld_json)
        except json.JSONDecodeError:
            ld_json = {}
    ld_json = _as_dict(ld_json)

    photo_ld = _as_dict(_as_dict(root).get("photoLdJSON"))
    images = (
        photo_ld.get("image", []) if isinstance(photo_ld.get("image"), list) else []
    )

    title = additional.get("title") or ld_json.get("name") or title_html
    if title:
        title = title.strip()
    else:
        title = None
    offers = _as_dict(ld_json.get("offers"))
    price_usd = offers.get("price")
    if price_usd is None:
        prices = _as_dict(additional.get("prices"))
        price_usd = prices.get("USD")
    if price_usd is None and title_html:
        price_usd = _parse_price_from_title(title_html)
    if price_usd is None and meta_description:
        price_usd = _parse_price_from_description(meta_description)

    mileage = _as_dict(ld_json.get("mileageFromOdometer"))
    odometer = mileage.get("value")
    if odometer is None and meta_description:
        odometer = _parse_odometer_from_description(meta_description)

    owner = _as_dict(additional.get("owner"))
    username = owner.get("name") or ""
    if not username and meta_description:
        username = _parse_username_from_description(meta_description) or ""
    username = username.strip() if username else None

    main_photo = _as_dict(additional.get("mainPhoto"))
    image_url = main_photo.get("src") or ""
    if not image_url:
        formats = _as_dict(main_photo.get("formats"))
        image_url = formats.get("large") or formats.get("middle") or ""
    if not image_url and images:
        first = images[0]
        if isinstance(first, dict):
            image_url = first.get("contentUrl") or first.get("image") or ""
    if not image_url and og_image:
        image_url = og_image
    image_url = image_url.strip() if image_url else None

    images_count = 0
    match = CAROUSEL_COUNT_RE.search(html)
    if match:
        images_count = int(match.group(1))
    if images_count == 0:
        images_count = len(images) if isinstance(images, list) else 0
    if images_count == 0:
        images_count = _first_int(_deep_find(additional, "count")) or 0

    car_vin = ld_json.get("vehicleIdentificationNumber")
    if not car_vin:
        car_vin = _first_str(_deep_find(pinia, "vin")) or _first_str(
            _deep_find(pinia, "car_vin")
        )
    if not car_vin and title_html:
        car_vin = _parse_vin_from_title(title_html)

    car_number = (
        _first_str(_deep_find(pinia, "carNumber"))
        or _first_str(_deep_find(pinia, "autoNumber"))
        or _first_str(_deep_find(pinia, "car_number"))
        or _first_str(_deep_find(pinia, "plateNumber"))
    )
    if not car_number and title_html:
        car_number = _parse_plate_from_title(title_html)

    phone_id = _first_str(_deep_find(pinia, "phoneId"))
    auto_id = additional.get("autoId") or _first_str(_deep_find(pinia, "autoId"))
    user_id = _first_str(_deep_find(pinia, "userId")) or owner.get("id")
    avatar = owner.get("photo") or _first_str(_deep_find(pinia, "avatar")) or ""
    phone_meta = None
    if auto_id and user_id and phone_id:
        phone_meta = {
            "auto_id": str(auto_id),
            "user_id": str(user_id),
            "phone_id": str(phone_id),
            "title": title or "",
            "avatar": avatar,
            "user_name": username or "",
        }

    data = {
        "url": url,
        "title": title,
        "price_usd": price_usd,
        "odometer": odometer,
        "username": username,
        "phone_number": None,
        "image_url": image_url,
        "images_count": images_count,
        "car_number": car_number,
        "car_vin": car_vin,
    }
    missing = [
        key
        for key, value in data.items()
        if key
        in {
            "title",
            "price_usd",
            "odometer",
            "username",
            "image_url",
            "images_count",
            "car_number",
            "car_vin",
        }
        and (value is None or value == "" or value == 0)
    ]
    return data, phone_meta, missing
