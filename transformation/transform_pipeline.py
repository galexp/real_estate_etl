import pandas as pd
import os

RAW_FILE = "data/raw/properties_austin_tx.csv"
OUT_DIR = "data/processed"

COLUMN_MAPPING = {
    "id": "property_id",
    "formattedAddress": "full_address",
    "addressLine1": "address_line1",
    "addressLine2": "address_line2",
    "city": "city",
    "state": "state",
    "stateFips": "state_fips",
    "zipCode": "zip_code",
    "county": "county",
    "countyFips": "county_fips",
    "latitude": "latitude",
    "longitude": "longitude",
    "propertyType": "property_type",
    "bedrooms": "bedrooms",
    "bathrooms": "bathrooms",
    "squareFootage": "square_footage",
    "yearBuilt": "year_built",
    "features": "features",
    "taxAssessments": "tax_assessments",
    "propertyTaxes": "property_taxes",
    "lotSize": "lot_size",
    "assessorID": "assessor_id",
    "legalDescription": "legal_description",
    "subdivision": "subdivision",
    "owner": "owner",
    "ownerOccupied": "owner_occupied"
}

def run_transformations():
    df = pd.read_csv(RAW_FILE)

    # Rename columns
    df = df.rename(columns=COLUMN_MAPPING)

    os.makedirs(OUT_DIR, exist_ok=True)
    df.to_csv(f"{OUT_DIR}/properties.csv", index=False)

    print("✅ Property transformation complete")
    print(f"Number of properties: {len(df)}")
    print("Columns after rename:", df.columns.tolist())

if __name__ == "__main__":
    run_transformations()
