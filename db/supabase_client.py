# db/supabase_client.py
from datetime import datetime
import re
import uuid
from supabase import create_client, Client
from supabase.client import ClientOptions
from typing import Dict, List, Any, Optional
import logging
from config import SUPABASE_URL, SUPABASE_KEY

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self):
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Supabase URL and API key must be provided in .env file")
        
        logger.info(f"Initializing Supabase client")
        
        # Initialize the Supabase client with options
        self.client: Client = create_client(
            SUPABASE_URL,
            SUPABASE_KEY,
            options=ClientOptions(
                postgrest_client_timeout=10,
                storage_client_timeout=10,
                schema="public",
            )
        )
    
    async def upsert_coffees(self, coffees_data: List[Dict[str, Any]]) -> List[str]:
        """
        Batch insert or update coffee products and return their IDs
        """
        try:
            if not coffees_data:
                return []

             # Convert datetime objects to strings before upserting
            for coffee in coffees_data:
                if 'created_at' in coffee and isinstance(coffee['created_at'], datetime):
                    coffee['created_at'] = coffee['created_at'].isoformat()
                if 'updated_at' in coffee and isinstance(coffee['updated_at'], datetime):
                    coffee['updated_at'] = coffee['updated_at'].isoformat()  
            
            response = self.client.table('coffees').upsert(
                coffees_data,
                on_conflict=['slug']
            ).execute()
            
            if response.data:
                return [item['id'] for item in response.data]
            else:
                logger.error(f"Failed to upsert coffees: {len(coffees_data)} items")
                return []
        except Exception as e:
            logger.error(f"Error upserting coffees: {str(e)}")
            raise
    
    async def upsert_region(self, region_name: str) -> Optional[str]:
        """
        Use the upsert_region RPC function to get or create a region
        """
        try:
            response = self.client.rpc(
                'upsert_region', 
                {'region_name': region_name}
            ).execute()
            
            if response.data:
                return response.data[0]
            return None
        except Exception as e:
            logger.error(f"Error upserting region: {str(e)}")
            return None
            
    async def link_flavor_profile(self, coffee_id: str, flavor_name: str) -> None:
        """
        Link a flavor profile to a coffee using the RPC function
        """
        try:
            self.client.rpc(
                'upsert_flavor_and_link',
                {'coffee': coffee_id, 'flavor_name': flavor_name.lower()}
            ).execute()
        except Exception as e:
            logger.error(f"Error linking flavor profile: {str(e)}")
            
    async def link_brew_method(self, coffee_id: str, method_name: str) -> None:
        """
        Link a brew method to a coffee using the RPC function
        """
        try:
            self.client.rpc(
                'upsert_brew_method_and_link',
                {'coffee': coffee_id, 'method_name': method_name.lower()}
            ).execute()
        except Exception as e:
            logger.error(f"Error linking brew method: {str(e)}")
    
    async def add_external_link(self, coffee_id: str, provider: str, url: str) -> None:
        """
        Add or update an external purchase link
        """
        try:
            self.client.rpc(
                'upsert_external_link',
                {'coffee': coffee_id, 'provider': provider, 'link': url}
            ).execute()
        except Exception as e:
            logger.error(f"Error adding external link: {str(e)}")
    
    async def upsert_coffee_prices(self, coffee_id: str, prices: Dict[int, float]) -> bool:
        """
        Insert or update coffee prices.
        
        Args:
            coffee_id: ID of the coffee
            prices: Dictionary of {size_grams: price}
            
        Returns:
            Success flag
        """
        try:
            if not coffee_id or not prices:
                return False
                
            # Convert dictionary to list of records
            price_records = [
                {"coffee_id": coffee_id, "size_grams": size, "price": price}
                for size, price in prices.items()
            ]
            
            # Upsert prices
            response = self.client.table('coffee_prices').upsert(
                price_records,
                on_conflict=['coffee_id', 'size_grams']
            ).execute()
            
            return response.data is not None
            
        except Exception as e:
            logger.error(f"Error upserting coffee prices: {str(e)}")
            return False
        
    async def upsert_roaster(self, roaster_data: Dict[str, Any]) -> Optional[str]:
        """
        Insert or update a roaster and return its ID.
        
        Args:
            roaster_data: Dictionary with roaster data
            
        Returns:
            Roaster ID if successful, None otherwise
        """
        try:
            if not roaster_data:
                logger.error("No roaster data provided for upsert")
                return None
                
            # Convert datetime objects to strings before upserting
            if 'created_at' in roaster_data and isinstance(roaster_data['created_at'], datetime):
                roaster_data['created_at'] = roaster_data['created_at'].isoformat()
            if 'updated_at' in roaster_data and isinstance(roaster_data['updated_at'], datetime):
                roaster_data['updated_at'] = roaster_data['updated_at'].isoformat()
            
            # Make sure ID is properly formatted
            if 'id' not in roaster_data or not roaster_data['id']:
                roaster_data['id'] = str(uuid.uuid4())
            
            # Ensure required fields are present
            required_fields = ['name', 'slug', 'website_url']
            for field in required_fields:
                if field not in roaster_data or not roaster_data[field]:
                    logger.error(f"Missing required field {field} in roaster data")
                    return None
            
            # Verify data types
            if isinstance(roaster_data.get('social_links'), list):
                # Ensure social_links is properly formatted for PostgreSQL
                roaster_data['social_links'] = list(filter(None, roaster_data['social_links']))
            

            # Set updated_at to current time
            roaster_data['updated_at'] = datetime.now().isoformat()
            
            # Upsert roaster
            response = self.client.table('roasters').upsert(
                roaster_data,
                on_conflict=['slug']
            ).execute()
            
            if response.data:
                return response.data[0]['id']
            else:
                logger.error(f"Failed to upsert roaster: {roaster_data.get('name')}")
                return None
                
        except Exception as e:
            logger.error(f"Error upserting roaster: {str(e)}")
            return None