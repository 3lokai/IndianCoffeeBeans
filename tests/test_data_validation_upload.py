import pytest
from pydantic import BaseModel, ValidationError
from unittest.mock import patch

# --- test_pydantic_model_validation ---
class ProductModel(BaseModel):
    name: str
    description: str
    image_url: str

def test_pydantic_model_validation():
    valid = {'name': 'Coffee', 'description': 'desc', 'image_url': 'img.jpg'}
    ProductModel(**valid)
    with pytest.raises(ValidationError):
        ProductModel(name='Coffee', description='desc')  # missing image_url

# --- test_batch_upsert_supabase ---
def test_batch_upsert_supabase():
    from scrapers.roaster_pipeline import batch_upsert_products
    products = [
        {'name': 'Coffee', 'description': 'desc', 'image_url': 'img.jpg'},
        {'name': 'Espresso', 'description': 'desc', 'image_url': 'img2.jpg'},
    ]
    with patch('scrapers.roaster_pipeline.supabase_client') as mock_client:
        batch_upsert_products(products)
        mock_client.table.assert_called_with('products')
        mock_client.upsert.assert_called_with(products)

# --- test_save_csv_copy ---
def test_save_csv_copy(tmp_path):
    from scrapers.roaster_pipeline import save_products_csv
    products = [
        {'name': 'Coffee', 'description': 'desc', 'image_url': 'img.jpg'},
        {'name': 'Espresso', 'description': 'desc', 'image_url': 'img2.jpg'},
    ]
    csv_path = tmp_path / 'products.csv'
    save_products_csv(products, csv_path)
    content = csv_path.read_text()
    assert 'Coffee' in content and 'Espresso' in content
