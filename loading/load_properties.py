import pandas as pd
import numpy as np
from db.postgres_client import PostgresClient


# --------------------------------------------------
# CREATE TABLE
# --------------------------------------------------
def create_properties_table(pg: PostgresClient):
    query = """
    CREATE TABLE IF NOT EXISTS properties (
        property_id TEXT PRIMARY KEY,
        full_address TEXT,
        address_line1 TEXT,
        address_line2 TEXT,
        city TEXT,
        state TEXT,
        state_fips TEXT,
        zip_code TEXT,
        county TEXT,
        county_fips TEXT,
        latitude NUMERIC,
        longitude NUMERIC,
        property_type TEXT,
        bedrooms NUMERIC,
        bathrooms NUMERIC,
        square_footage NUMERIC,
        year_built NUMERIC,
        features TEXT,
        tax_assessments TEXT,
        property_taxes TEXT,
        lot_size TEXT,
        assessor_id TEXT,
        legal_description TEXT,
        subdivision TEXT,
        owner TEXT,
        owner_occupied BOOLEAN,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """
    with pg.conn.cursor() as cur:
        cur.execute(query)
        pg.conn.commit()

    print("✅ properties table ready")


# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
def load_properties(csv_path: str, pg: PostgresClient):

    # Ensure table exists
    create_properties_table(pg)

    # Read CSV
    df = pd.read_csv(csv_path)

    existing_ids = pg.fetch_existing_property_ids()
    print(f"📦 {len(existing_ids)} existing properties found in DB")

    # Incremental load: keep only NEW properties
    initial_count = len(df)
    df = df[~df["property_id"].isin(existing_ids)]
    filtered_count = len(df)

    print(f"🆕 New properties to load: {filtered_count} (skipped {initial_count - filtered_count})")

    if df.empty:
        print("✅ No new properties to load. Incremental load complete.")
        return
    
    # Optional: save incremental snapshot
    df.to_csv("data/processed/new_properties.csv", index=False)


    # --------------------------------------------------
    # DATA TYPE FIXES (CRITICAL)
    # --------------------------------------------------

    # --- Boolean handling (THIS FIXES YOUR ERROR) ---
    if "owner_occupied" in df.columns:
        df["owner_occupied"] = df["owner_occupied"].replace({np.nan: None})
        df["owner_occupied"] = df["owner_occupied"].apply(
            lambda x: True if x is True or x == 1
            else False if x is False or x == 0
            else None
        )
        df["owner_occupied"] = df["owner_occupied"].astype(object)

    # --- Numeric columns ---
    numeric_cols = [
        "bedrooms",
        "bathrooms",
        "square_footage",
        "year_built",
        "latitude",
        "longitude"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: float(x) if pd.notna(x) else None)

    # --------------------------------------------------
    # INSERT QUERY
    # --------------------------------------------------
    insert_query = """
    INSERT INTO properties (
        property_id,
        full_address,
        address_line1,
        address_line2,
        city,
        state,
        state_fips,
        zip_code,
        county,
        county_fips,
        latitude,
        longitude,
        property_type,
        bedrooms,
        bathrooms,
        square_footage,
        year_built,
        features,
        tax_assessments,
        property_taxes,
        lot_size,
        assessor_id,
        legal_description,
        subdivision,
        owner,
        owner_occupied
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (property_id)
    DO UPDATE SET
        full_address = EXCLUDED.full_address,
        address_line1 = EXCLUDED.address_line1,
        address_line2 = EXCLUDED.address_line2,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        state_fips = EXCLUDED.state_fips,
        zip_code = EXCLUDED.zip_code,
        county = EXCLUDED.county,
        county_fips = EXCLUDED.county_fips,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        property_type = EXCLUDED.property_type,
        bedrooms = EXCLUDED.bedrooms,
        bathrooms = EXCLUDED.bathrooms,
        square_footage = EXCLUDED.square_footage,
        year_built = EXCLUDED.year_built,
        features = EXCLUDED.features,
        tax_assessments = EXCLUDED.tax_assessments,
        property_taxes = EXCLUDED.property_taxes,
        lot_size = EXCLUDED.lot_size,
        assessor_id = EXCLUDED.assessor_id,
        legal_description = EXCLUDED.legal_description,
        subdivision = EXCLUDED.subdivision,
        owner = EXCLUDED.owner,
        owner_occupied = EXCLUDED.owner_occupied,
        updated_at = NOW();
    """

    # --------------------------------------------------
    # BUILD DATA TUPLES (SAFE)
    # --------------------------------------------------
    data = [
        (
            row.property_id,
            row.full_address,
            row.address_line1,
            row.address_line2,
            row.city,
            row.state,
            row.state_fips,
            row.zip_code,
            row.county,
            row.county_fips,
            row.latitude,
            row.longitude,
            row.property_type,
            row.bedrooms,
            row.bathrooms,
            row.square_footage,
            row.year_built,
            row.features,
            row.tax_assessments,
            row.property_taxes,
            row.lot_size,
            row.assessor_id,
            row.legal_description,
            row.subdivision,
            row.owner,
            row.owner_occupied
        )
        for row in df.itertuples(index=False)
    ]

    # --------------------------------------------------
    # EXECUTE
    # --------------------------------------------------
    pg.execute_batch(insert_query, data)

    print(f"✅ {len(data)} properties loaded successfully")
