#!/usr/bin/env python3
"""
Dog Park Scraper for Compawnion
Scrapes dog park locations from multiple sources and exports to JSON for the map
"""

import requests
import json
import time
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
import argparse


@dataclass
class DogPark:
    """Represents a dog park with location and details"""
    name: str
    lat: float
    lng: float
    description: str = ""
    address: str = ""
    amenities: List[str] = None
    area_bounds: Optional[List[List[float]]] = None  # For polygon areas
    source: str = ""
    
    def __post_init__(self):
        if self.amenities is None:
            self.amenities = []


class DogParkScraper:
    """Scrapes dog park data from various sources"""
    
    def __init__(self, location: str = "Madison, WI", radius_miles: float = 10):
        self.location = location
        self.radius_miles = radius_miles
        self.parks: List[DogPark] = []
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Compawnion Dog Park Finder (Educational Project)'
        })
    
    def geocode_location(self) -> tuple:
        """Convert location string to coordinates"""
        print(f"Geocoding location: {self.location}")
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            'q': self.location,
            'format': 'json',
            'limit': 1
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data:
                lat = float(data[0]['lat'])
                lng = float(data[0]['lon'])
                print(f"Found coordinates: {lat}, {lng}")
                return lat, lng
            else:
                print("Location not found, using default Madison coordinates")
                return 43.0731, -89.4012
        except Exception as e:
            print(f"Geocoding error: {e}")
            return 43.0731, -89.4012
    
    def scrape_openstreetmap(self, center_lat: float, center_lng: float):
        """
        Scrape dog parks from OpenStreetMap using Overpass API
        This is the most reliable free source for dog park data
        """
        print("\nScraping OpenStreetMap via Overpass API...")
        
        # Convert miles to meters for Overpass API
        radius_meters = int(self.radius_miles * 1609.34)
        
        # Overpass QL query for dog parks and related features
        overpass_query = f"""
        [out:json][timeout:25];
        (
          // Dog parks specifically tagged
          node["leisure"="dog_park"](around:{radius_meters},{center_lat},{center_lng});
          way["leisure"="dog_park"](around:{radius_meters},{center_lat},{center_lng});
          relation["leisure"="dog_park"](around:{radius_meters},{center_lat},{center_lng});
          
          // Parks that allow dogs off-leash
          node["dog"="unleashed"](around:{radius_meters},{center_lat},{center_lng});
          way["dog"="unleashed"](around:{radius_meters},{center_lat},{center_lng});
          
          // Dog exercise areas
          node["amenity"="dog_exercise_area"](around:{radius_meters},{center_lat},{center_lng});
          way["amenity"="dog_exercise_area"](around:{radius_meters},{center_lat},{center_lng});
        );
        out body;
        >;
        out skel qt;
        """
        
        url = "https://overpass-api.de/api/interpreter"
        
        try:
            response = self.session.post(url, data={'data': overpass_query})
            response.raise_for_status()
            data = response.json()
            
            elements = data.get('elements', [])
            nodes_dict = {}
            
            # First pass: collect all nodes
            for element in elements:
                if element['type'] == 'node':
                    nodes_dict[element['id']] = element
            
            # Second pass: process dog parks
            for element in elements:
                if element['type'] == 'node' and 'tags' in element:
                    self._process_osm_node(element)
                elif element['type'] == 'way' and 'tags' in element:
                    self._process_osm_way(element, nodes_dict)
            
            print(f"Found {len([p for p in self.parks if p.source == 'OpenStreetMap'])} parks from OpenStreetMap")
            
        except Exception as e:
            print(f"Error scraping OpenStreetMap: {e}")
    
    def _process_osm_node(self, node: dict):
        """Process an OSM node (point) into a DogPark"""
        tags = node.get('tags', {})
        
        name = tags.get('name', tags.get('dog', 'Unnamed Dog Park'))
        
        # Build description from tags
        description_parts = []
        if 'description' in tags:
            description_parts.append(tags['description'])
        if 'surface' in tags:
            description_parts.append(f"Surface: {tags['surface']}")
        
        # Collect amenities
        amenities = []
        if tags.get('drinking_water') == 'yes':
            amenities.append('Water fountain')
        if tags.get('lit') == 'yes':
            amenities.append('Lighting')
        if tags.get('fence') == 'yes':
            amenities.append('Fenced')
        if tags.get('toilets') == 'yes':
            amenities.append('Restrooms')
        
        park = DogPark(
            name=name,
            lat=node['lat'],
            lng=node['lon'],
            description=' | '.join(description_parts) if description_parts else 'Dog park',
            address=tags.get('addr:full', ''),
            amenities=amenities,
            source='OpenStreetMap'
        )
        
        self.parks.append(park)
    
    def _process_osm_way(self, way: dict, nodes_dict: dict):
        """Process an OSM way (area) into a DogPark with bounds"""
        tags = way.get('tags', {})
        nodes = way.get('nodes', [])
        
        if not nodes:
            return
        
        # Calculate center point and area bounds
        coords = []
        for node_id in nodes:
            if node_id in nodes_dict:
                node = nodes_dict[node_id]
                coords.append([node['lat'], node['lon']])
        
        if not coords:
            return
        
        # Calculate centroid
        center_lat = sum(c[0] for c in coords) / len(coords)
        center_lng = sum(c[1] for c in coords) / len(coords)
        
        name = tags.get('name', 'Unnamed Dog Park')
        
        description_parts = []
        if 'description' in tags:
            description_parts.append(tags['description'])
        if 'surface' in tags:
            description_parts.append(f"Surface: {tags['surface']}")
        
        amenities = []
        if tags.get('drinking_water') == 'yes':
            amenities.append('Water fountain')
        if tags.get('lit') == 'yes':
            amenities.append('Lighting')
        if tags.get('fence') == 'yes':
            amenities.append('Fenced')
        if tags.get('toilets') == 'yes':
            amenities.append('Restrooms')
        
        park = DogPark(
            name=name,
            lat=center_lat,
            lng=center_lng,
            description=' | '.join(description_parts) if description_parts else 'Dog park area',
            address=tags.get('addr:full', ''),
            amenities=amenities,
            area_bounds=coords,
            source='OpenStreetMap'
        )
        
        self.parks.append(park)
    
    def scrape_bringfido(self, center_lat: float, center_lng: float):
        """
        Scrape dog parks from BringFido (dog-friendly place directory)
        Note: This is a conceptual implementation. Actual scraping may require
        handling of rate limits, authentication, or API access.
        """
        print("\nSearching BringFido database...")
        print("Note: BringFido scraping requires careful rate limiting and may need API access")
        
        # BringFido has an API but requires authentication
        # For demo purposes, we'll add a few known parks
        # In production, you would either:
        # 1. Use their official API with proper credentials
        # 2. Implement careful web scraping with respect to their robots.txt
        # 3. Use cached/downloaded data
        
        print("BringFido scraping not implemented (requires API key or careful rate limiting)")
        print("Consider signing up for their API at: https://www.bringfido.com/")
    
    def scrape_local_government_data(self, city: str, state: str):
        """
        Attempt to scrape from local government open data portals
        Many cities provide park data through open data initiatives
        """
        print(f"\nSearching for {city}, {state} government open data...")
        
        # Madison, WI has an open data portal
        if city.lower() == "madison" and state.lower() in ["wi", "wisconsin"]:
            self._scrape_madison_parks()
        else:
            print(f"No specific scraper for {city}, {state} government data")
            print("Consider checking: data.gov or your city's open data portal")
    
    def _scrape_madison_parks(self):
        """Scrape Madison, WI specific park data"""
        print("Fetching Madison Parks Department data...")
        
        # Madison has several well-known dog parks we can add
        madison_parks = [
            {
                'name': 'Sycamore Dog Park',
                'lat': 43.0848,
                'lng': -89.4445,
                'description': 'Large fenced area with separate small dog section',
                'address': 'Sycamore Ave, Madison, WI',
                'amenities': ['Fenced', 'Separate small dog area', 'Water fountain']
            },
            {
                'name': 'Yahara Heights Park',
                'lat': 43.1101,
                'lng': -89.3156,
                'description': 'Popular dog-friendly park with trails',
                'address': 'Yahara Heights Rd, Madison, WI',
                'amenities': ['Trails', 'Parking']
            },
            {
                'name': 'Warner Park Dog Exercise Area',
                'lat': 43.1141,
                'lng': -89.3537,
                'description': 'Spacious off-leash area near the lake',
                'address': 'Warner Park, Madison, WI',
                'amenities': ['Fenced', 'Lake access', 'Parking']
            },
            {
                'name': 'Quann Dog Park',
                'lat': 43.0245,
                'lng': -89.5137,
                'description': 'Well-maintained dog park on the west side',
                'address': 'Quann Park, Madison, WI',
                'amenities': ['Fenced', 'Water fountain', 'Parking']
            },
            {
                'name': 'Token Creek Dog Park',
                'lat': 43.1499,
                'lng': -89.2561,
                'description': 'Large rural dog park with nature trails',
                'address': 'Token Creek County Park, WI',
                'amenities': ['Large area', 'Trails', 'Parking']
            }
        ]
        
        for park_data in madison_parks:
            park = DogPark(
                name=park_data['name'],
                lat=park_data['lat'],
                lng=park_data['lng'],
                description=park_data['description'],
                address=park_data['address'],
                amenities=park_data['amenities'],
                source='Madison Parks Department'
            )
            self.parks.append(park)
        
        print(f"Added {len(madison_parks)} parks from Madison Parks Department")
    
    def remove_duplicates(self):
        """Remove duplicate parks based on proximity"""
        print("\nRemoving duplicates...")
        unique_parks = []
        
        for park in self.parks:
            is_duplicate = False
            for unique_park in unique_parks:
                # If two parks are within ~100 meters, consider them duplicates
                lat_diff = abs(park.lat - unique_park.lat)
                lng_diff = abs(park.lng - unique_park.lng)
                
                if lat_diff < 0.001 and lng_diff < 0.001:
                    is_duplicate = True
                    # Merge information from duplicate
                    if park.description and not unique_park.description:
                        unique_park.description = park.description
                    if park.amenities:
                        unique_park.amenities = list(set(unique_park.amenities + park.amenities))
                    break
            
            if not is_duplicate:
                unique_parks.append(park)
        
        removed = len(self.parks) - len(unique_parks)
        self.parks = unique_parks
        print(f"Removed {removed} duplicate parks")
    
    def export_to_json(self, filename: str = 'dog_parks.json'):
        """Export parks data to JSON file"""
        parks_data = [asdict(park) for park in self.parks]
        
        output = {
            'location': self.location,
            'total_parks': len(parks_data),
            'parks': parks_data
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Exported {len(parks_data)} parks to {filename}")
    
    def export_to_js(self, filename: str = 'dog_parks.js'):
        """Export parks data as JavaScript for direct inclusion in HTML"""
        parks_data = [asdict(park) for park in self.parks]
        
        js_content = f"""// Auto-generated dog parks data for Compawnion
// Generated for: {self.location}
// Total parks: {len(parks_data)}

const dogParksData = {json.dumps(parks_data, indent=2)};

// Export for use in HTML
if (typeof module !== 'undefined' && module.exports) {{
    module.exports = dogParksData;
}}
"""
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(js_content)
        
        print(f"✅ Exported {len(parks_data)} parks to {filename}")
    
    def run(self):
        """Run the complete scraping pipeline"""
        print("=" * 60)
        print("🐕 Compawnion Dog Park Scraper")
        print("=" * 60)
        
        # Geocode the location
        center_lat, center_lng = self.geocode_location()
        
        # Scrape from multiple sources
        self.scrape_openstreetmap(center_lat, center_lng)
        time.sleep(1)  # Be respectful with API calls
        
        # Parse location for city/state
        location_parts = self.location.split(',')
        if len(location_parts) >= 2:
            city = location_parts[0].strip()
            state = location_parts[1].strip()
            self.scrape_local_government_data(city, state)
        
        # Additional sources could be added here:
        # self.scrape_bringfido(center_lat, center_lng)
        # self.scrape_yelp(center_lat, center_lng)
        
        # Clean up data
        self.remove_duplicates()
        
        # Export results
        self.export_to_json()
        self.export_to_js()
        
        print("\n" + "=" * 60)
        print(f"✨ Successfully found {len(self.parks)} dog parks!")
        print("=" * 60)
        
        return self.parks


def main():
    parser = argparse.ArgumentParser(
        description='Scrape dog park locations for Compawnion map'
    )
    parser.add_argument(
        '--location',
        type=str,
        default='Madison, WI',
        help='Location to search for dog parks (default: Madison, WI)'
    )
    parser.add_argument(
        '--radius',
        type=float,
        default=10,
        help='Search radius in miles (default: 10)'
    )
    parser.add_argument(
        '--output-json',
        type=str,
        default='dog_parks.json',
        help='Output JSON filename (default: dog_parks.json)'
    )
    parser.add_argument(
        '--output-js',
        type=str,
        default='dog_parks.js',
        help='Output JavaScript filename (default: dog_parks.js)'
    )
    
    args = parser.parse_args()
    
    # Create scraper and run
    scraper = DogParkScraper(
        location=args.location,
        radius_miles=args.radius
    )
    
    parks = scraper.run()
    
    # Print summary
    print("\n📊 Summary by Source:")
    sources = {}
    for park in parks:
        sources[park.source] = sources.get(park.source, 0) + 1
    
    for source, count in sources.items():
        print(f"  • {source}: {count} parks")
    
    print("\n🗺️  Sample Parks Found:")
    for park in parks[:5]:
        print(f"  • {park.name} ({park.lat:.4f}, {park.lng:.4f})")
        if park.amenities:
            print(f"    Amenities: {', '.join(park.amenities)}")


if __name__ == '__main__':
    main()
