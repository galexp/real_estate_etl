import pandas as pd


def run_quality_checks():
    """
    Runs basic data quality checks on processed property data.
    This validates schema, nulls, duplicates, and basic value sanity.
    """

    print("🔍 Running data quality checks...")

    # Load processed data (same file used for DB load)
    df = pd.read_csv("data/processed/properties.csv")

    # ----------------------------------
    # 1. Required Columns Check
    # ----------------------------------
    required_columns = [
        "property_id",
        "full_address",
        "city",
        "state",
        "property_type"
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"❌ Missing required columns: {missing_columns}")

    print("✅ Required columns present")

    # ----------------------------------
    # 2. Null Value Checks
    # ----------------------------------
    critical_nulls = df[required_columns].isnull().sum()

    print("\n📉 Null counts (critical fields):")
    print(critical_nulls)

    if critical_nulls.any():
        print("⚠️ Warning: Null values found in critical fields")

    # ----------------------------------
    # 3. Duplicate Property ID Check
    # ----------------------------------
    duplicate_count = df.duplicated(subset=["property_id"]).sum()

    print(f"\n🔁 Duplicate property_id count: {duplicate_count}")

    if duplicate_count > 0:
        raise ValueError("❌ Duplicate property_id values detected")

    print("✅ No duplicate property IDs")

    # ----------------------------------
    # 4. Numeric Value Sanity Checks
    # ----------------------------------
    numeric_checks = {
        "bedrooms": (0, 50),
        "bathrooms": (0, 50),
        "square_footage": (100, 50000),
        "year_built": (1700, 2100)
    }

    for col, (min_val, max_val) in numeric_checks.items():
        if col in df.columns:
            invalid_count = df[
                (df[col].notna()) &
                ((df[col] < min_val) | (df[col] > max_val))
            ].shape[0]

            if invalid_count > 0:
                print(f"⚠️ {invalid_count} invalid values detected in {col}")

    print("✅ Numeric sanity checks complete")

    # ----------------------------------
    # 5. Boolean Column Validation
    # ----------------------------------
    if "owner_occupied" in df.columns:
        invalid_booleans = df[
            ~df["owner_occupied"].isin([True, False]) & df["owner_occupied"].notna()
        ].shape[0]

        if invalid_booleans > 0:
            raise ValueError("❌ Invalid boolean values in owner_occupied")

        print("✅ Boolean values validated")

    print("\n🎉 Data quality checks PASSED")
