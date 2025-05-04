# ☕ Supabase Schema for IndianCoffeeBeans.com

### Updated: May 2025

---

## 📍 Regions

| Column           | Type     | Notes                          |
|------------------|----------|--------------------------------|
| id               | UUID     | Primary key                    |
| name             | TEXT     | Unique region name             |
| origin_country   | TEXT     | Defaults to 'India'            |
| altitude_meters  | INT      | Optional elevation             |

---

## 🏭 Roasters

| Column             | Type      | Notes                                  |
|--------------------|-----------|----------------------------------------|
| id                 | UUID      | Primary key                            |
| name               | TEXT      | Unique name                            |
| slug               | TEXT      | Unique slug                            |
| description        | TEXT      | Optional                                |
| website_url        | TEXT      | Roaster’s website                      |
| social_links       | TEXT[]    | Instagram, Facebook, etc.              |
| contact_email      | TEXT      | Optional                                |
| contact_phone      | TEXT      | Optional                                |
| city               | TEXT      | Optional                                |
| state              | TEXT      | Optional                                |
| founded_year       | INT       | Optional                                |
| logo_url           | TEXT      | Optional                                |
| has_subscription   | BOOLEAN   | Default: false                          |
| has_physical_store | BOOLEAN   | Default: false                          |
| is_featured        | BOOLEAN   | Default: false                          |
| created_at         | TIMESTAMP | Defaults to now()                       |
| updated_at         | TIMESTAMP | Defaults to now()                       |
| instagram_handle    | TEXT      | Optional (parsed from social links) |

---

## ☕ Coffees

| Column             | Type      | Notes                                           |
|--------------------|-----------|-------------------------------------------------|
| id                 | UUID      | Primary key                                     |
| roaster_id         | UUID      | Foreign key → roasters(id)                      |
| name               | TEXT      | Required                                        |
| slug               | TEXT      | Unique                                          |
| description        | TEXT      | Optional                                        |
| roast_level        | TEXT      | Enum check                                      |
| bean_type          | TEXT      | Enum check                                      |
| processing_method  | TEXT      | Enum check                                      |
| image_url          | TEXT      | Optional                                        |
| direct_buy_url     | TEXT      | Optional                                        |
| region_id          | UUID      | Foreign key → regions(id), nullable            |
| is_seasonal        | BOOLEAN   | Default: false                                  |
| is_available       | BOOLEAN   | Default: true                                   |
| is_featured        | BOOLEAN   | Default: false                                  |
| is_single_origin   | BOOLEAN   | Default: true                                   |
| tags               | TEXT[]    | Optional free-form tags                         |
| deepseek_enriched  | BOOLEAN   | Default: false (true if LLM fallback used)      |
| created_at         | TIMESTAMP | Defaults to now()                               |
| updated_at         | TIMESTAMP | Defaults to now()                               |

**Indexes**
- `slug`
- `direct_buy_url`

---

## 💸 Coffee Prices

Composite key on `(coffee_id, size_grams)`.

| Column      | Type    |
|-------------|---------|
| coffee_id   | UUID    |
| size_grams  | INT     |
| price       | NUMERIC |

---

## 🎨 Flavor Profiles (many-to-many)

- `flavor_profiles`: master table
- `coffee_flavor_profiles`: join table

---

## 🧪 Brew Methods (many-to-many)

- `brew_methods`: master table
- `coffee_brew_methods`: join table

---

## 🌐 External Purchase Links

| Column     | Type  |
|------------|-------|
| provider   | TEXT  |
| url        | TEXT  |

---

## 🛠 Supabase Functions for Scraper

These Postgres functions simplify backend logic by performing insert-or-select operations and managing many-to-many relationships in one call.

---

### 🔁 `upsert_flavor_and_link(coffee UUID, flavor_name TEXT)`

* Inserts a flavor into `flavor_profiles` (if it doesn’t exist).
* Links it to a coffee via `coffee_flavor_profiles` (if not already linked).

---

### 🔁 `upsert_brew_method_and_link(coffee UUID, method_name TEXT)`

* Inserts a brew method into `brew_methods`.
* Links it to a coffee via `coffee_brew_methods`.

---

### 🌐 `upsert_external_link(coffee UUID, provider TEXT, link TEXT)`

* Inserts or updates a link in `external_links`.
* Uses `(coffee_id, provider)` as the uniqueness check.

---

### 🗺️ `upsert_region(region_name TEXT) RETURNS UUID`

* Inserts a region by name if not exists.
* Returns the `region_id` for use in `coffees.region_id`.

---

### ✅ Example Usage in Python (`supabase-py`)

```python
# Link a flavor
supabase.rpc('upsert_flavor_and_link', {
    'coffee': coffee_id,
    'flavor_name': 'citrus'
})

# Link a brew method
supabase.rpc('upsert_brew_method_and_link', {
    'coffee': coffee_id,
    'method_name': 'french press'
})

# Add/update external link
supabase.rpc('upsert_external_link', {
    'coffee': coffee_id,
    'provider': 'Amazon',
    'link': 'https://amazon.in/...'
})

# Get region_id and link it to coffee
region_id = supabase.rpc('upsert_region', {
    'region_name': 'Araku'
}).execute().data
```

---