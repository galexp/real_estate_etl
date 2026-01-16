from extraction.rentcast_client import RentCastAPIClient
from transformation.transform_pipeline import run_transformations
from db.postgres_client import PostgresClient
from loading.load_properties import load_properties
from quality.quality_pipeline import run_quality_checks
import os

def run_etl():
    # Extraction
    client = RentCastAPIClient()
    df = client.get_property_listings("Austin", "TX", limit=300)
    print(f"✅ Extracted {len(df)} listings")

    # Transformation
    run_transformations()

    # Load
    pg = PostgresClient(
        host="localhost",
        db="amdaridb",
        user="rentcast_user",
        password="YourPassword123"
    )
    load_properties("data/processed/properties.csv", pg)
    pg.close()

    # Quality Checks
    run_quality_checks()

if __name__ == "__main__":
    run_etl()
