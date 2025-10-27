import pandas as pd
import difflib
import logging
from dash import Dash, dcc, html, Input, Output, State
from io import BytesIO
import base64
import re
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(level=logging.INFO, filename='sugar_procurement_chatbot.log',
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Static city coordinates
city_coords = {
    'Anand': (22.5645, 72.9281), 'Kolkata': (22.5726, 88.3639), 'Mumbai': (19.0760, 72.8777),
    'New Delhi': (28.6139, 77.2090), 'Hyderabad': (17.3850, 78.4867), 'Chennai': (13.0827, 80.2707),
    'Patna': (25.5941, 85.1376), 'Jaipur': (26.9124, 75.7873), 'Bangalore': (12.9716, 77.5946),
    'Ahmedabad': (23.0225, 72.5714), 'Pune': (18.5204, 73.8567), 'Guwahati': (26.1445, 91.7362),
    'Indore': (22.7196, 75.8577), 'Nagpur': (21.1458, 79.0882), 'Coimbatore': (11.0168, 76.9558),
    'Kanpur': (26.76, 80.3), 'Visakhapatnam': (17.7865, 83.21185), 'Bhopal': (23.2599, 77.4126),
    'Amravati': (20.64, 77.752), 'Gorakhpur': (26.7606, 83.3732), 'Navi Mumbai': (19, 73),
    'Navsari': (20.9467, 72.9230), 'Didwana': (27.4, 74.5667), 'Surendranagar': (22.7201, 71.6495),
    'Rohtak': (28.8955, 76.6066), 'Jati': (20.1597, 85.7071), 'Nawa City': (25.0195, 75.0023),
    'Bishalgarh': (23.6766, 91.2757), 'Barauni': (25.4715, 85.9756), 'Gaya': (24.7914, 85),
    'Jind': (29.3211, 76.3058), 'Gurgaon': (28.4595, 77.0266), 'Begusarai': (25.4167, 86.1294),
    'Hisar': (29.1492, 75.7217), 'Noida': (28.5355, 77.3910), 'Pipariya': (22.7629, 78.3520),
    'Shahjahanpur': (27.8793, 79.9120), 'Jamshedpur': (22.8046, 86.2029), 'Tirora': (21.4085, 79.9326),
    'Cuttack': (20.4650, 85.8793), 'Bhiwandi': (19.2967, 73.0631), 'Purnia': (25.7771, 87.4753),
    'Muzaffarpur': (26.1209, 85.3647), 'Raipur': (21.2514, 81.6296), 'Erode': (11.3410, 77.7172),
    'Meerut': (28.9845, 77.7064), 'Karnal': (29.6857, 76.9905), 'Ambala': (30.3782, 76.7767),
    'Shahabad': (30.1677, 76.8699), 'Parwanoo': (30.8387, 76.9630), 'Amritsar': (31.6340, 74.8723),
    'Satara': (17.6805, 74.0183), 'Kolhapur': (16.6950, 74.2317), 'Palakkad': (10.7867, 76.6548),
    'Kollam': (8.8932, 76.6141), 'Ernakulam': (9.9816, 76.2999), 'Aligarh': (27.8974, 78.0880),
    'Puducherry': (11.9416, 79.8083), 'Thane': (19.2183, 72.9781), 'Ghaziabad': (28.6692, 77.4538),
    'Saharanpur': (29.9640, 77.5452), 'Gandhidham': (23.0753, 70.1337), 'Kaithal': (29.7954, 76.3996),
    'Ahmednagar': (19.0948, 74.7480), 'Kukarmunda': (21.5167, 74.3167), 'Bijnor': (29.3724, 78.1366),
    'Shamli': (29.4496, 77.3127), 'Royapettah': (13.0550, 80.2639), 'Secunderabad': (17.4399, 78.4983),
    'Vadodara': (22.3072, 73.1812)
}

# Load data
def load_data(file_path='Sugar Data.xlsx'):
    logger.info(f"Loading data from {file_path}")
    try:
        if not pd.io.common.file_exists(file_path):
            raise FileNotFoundError(f"Data file not found at: {file_path}")
        
        df = pd.read_excel(file_path, engine='openpyxl')
        required_columns = [
            'Rank', 'Auction Id', 'Auction Ord No.', 'Auction Date', 'Market Code', 'Location',
            'Initiator', 'CMID', 'Bidder Name', 'Bidder City', 'Bidder State', 'Lowest Price',
            'Quantity', 'Product', 'Product Description', 'Auction Description', 'Order Status',
            'Bid Count', 'Rejection Reason', 'Product Name'
        ]
        
        df.columns = [col.strip().title() for col in df.columns]
        missing_columns = [col for col in required_columns if col.title() not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns: {missing_columns}")
        
        df.columns = required_columns
        df['Auction Date'] = pd.to_datetime(df['Auction Date'], errors='coerce')
        df['Product Name'] = df['Product Name'].astype(str).str.strip().replace('nan', 'Unknown')
        df['Location'] = df['Location'].astype(str).str.strip().str.title()
        df['Bidder Name'] = df['Bidder Name'].astype(str).str.strip()
        df['Initiator'] = df['Initiator'].astype(str).str.strip().str.title()
        df['Bidder City'] = df['Bidder City'].astype(str).str.strip().str.title().replace({
            'New  Delhi': 'New Delhi', ' Delhi': 'New Delhi', 'Punaura': 'Purnia',
            'Muzzarpur': 'Muzaffarpur', 'Bhiwadi': 'Bhiwandi', 'Shahabad Markanda': 'Shahabad',
            'Ambala Cantt': 'Ambala'
        })
        df['Bidder State'] = df['Bidder State'].astype(str).str.strip().str.title().replace('nan', 'Unknown')
        df['Lowest Price'] = pd.to_numeric(df['Lowest Price'], errors='coerce')
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
        df['Rank'] = pd.to_numeric(df['Rank'], errors='coerce')
        df['Rejection Reason'] = df['Rejection Reason'].astype(str).str.strip()
        df['Auction Ord No.'] = df['Auction Ord No.'].astype(str).str.strip()
        
        df['Normalized Product'] = df['Product Name'].str.upper().str.strip().replace({
            'M30': 'M-30', 'M31': 'M-31', 'S30': 'S-30', 'S31': 'S-31', 'L30': 'L-30', 'L31': 'L-31',
            'M 30': 'M-30', 'M 31': 'M-31', 'S 30': 'S-30', 'S 31': 'S-31', 'L 30': 'L-30', 'L 31': 'L-31',
            'M-30SUGAR': 'M-30', 'S-30SUGAR': 'S-30', 'M30/31': 'M-30', 'S1-30': 'S-30',
            'PHARMA GRADE': 'PHARMA GRADE', 'PHARMAGRADE': 'PHARMA GRADE',
            'DOUBLE REFINED SUGAR': 'DOUBLE REFINED SUGAR', 'DOUBLEREFINEDSUGAR': 'DOUBLE REFINED SUGAR',
            'BRANDED SUGAR': 'BRANDED SUGAR', 'BRANDEDSUGAR': 'BRANDED SUGAR',
            'DEXTROSE MONOHYDRATE': 'DEXTROSE MONOHYDRATE', 'DEXTROSEMONOHYDRATE': 'DEXTROSE MONOHYDRATE',
            'G100': 'G-100', 'G60': 'G-60', 'G 100': 'G-100', 'G 60': 'G-60'
        }).str.replace(r'\(.*\)', '', regex=True).replace('UNKNOWN', 'Unknown')
        
        if df['Bidder City'].isna().any():
            df['Bidder City'] = df['Bidder City'].fillna('Unknown')
        if df['Auction Date'].isna().any():
            df = df.dropna(subset=['Auction Date'])
        if df['Auction Ord No.'].isna().any():
            df['Auction Ord No.'] = df['Auction Ord No.'].fillna('Unknown')
        
        if df.empty:
            raise ValueError("DataFrame empty after cleaning")
        
        logger.info(f"Loaded {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

# Parse prompt
def parse_prompt(prompt, known_locations, known_products):
    logger.info(f"Parsing prompt: '{prompt}'")
    prompt_clean = prompt.strip()
    match = re.match(r'^(.+?)\s*for\s*(.+)$', prompt_clean, re.IGNORECASE)
    if not match:
        logger.error("Invalid prompt format")
        return None, None
    
    product_input = match.group(1).strip()
    location_input = match.group(2).title().strip()
    
    if product_input.lower() == 'any sugar':
        product = 'Any Sugar'
    else:
        product_match = difflib.get_close_matches(product_input.upper(), known_products, n=1, cutoff=0.8)
        product = product_match[0] if product_match else None
    
    location_match = difflib.get_close_matches(location_input, known_locations + list(city_coords.keys()), n=1, cutoff=0.8)
    location = location_match[0] if location_match else None
    
    logger.info(f"Parsed product: '{product}', location: '{location}'")
    return product, location

# Calculate distance
def calculate_distance(loc1, loc2):
    try:
        from math import radians, sin, cos, sqrt, atan2
        R = 6371  # Earth's radius in km
        lat1, lon1 = loc1
        lat2, lon2 = loc2
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return round(R * c, 2)
    except Exception as e:
        logger.error(f"Distance calculation failed: {e}")
        return float('inf')

# Process requirement
def process_requirement(df, product, location):
    if df.empty:
        return {"error": "No data available"}
    
    loc_coords = city_coords.get(location, (20.5937, 78.9629))  # Default to India center
    try:
        if product == "Any Sugar":
            bidders = df.copy()
        else:
            bidders = df[df['Normalized Product'] == product.upper()].copy()
        
        if bidders.empty and product != "Any Sugar":
            available_products = sorted(df['Normalized Product'].unique())
            return {"error": f"No bidders found for {product}. Try: {', '.join(available_products)}"}
        
        current_date = pd.to_datetime('2025-06-03')
        six_months_ago = current_date - timedelta(days=180)
        
        bidder_agg = bidders.groupby(['Bidder Name', 'Bidder City', 'Bidder State']).agg({
            'Auction Ord No.': lambda x: len(set(x)),
            'Rank': lambda x: sum(x == 1),
            'Auction Date': [
                lambda x: len(x),
                lambda x: max(x),
                lambda x: any(x >= six_months_ago)
            ],
            'Lowest Price': 'mean',
            'Initiator': lambda x: sorted(set(x)),
            'Product Name': lambda x: sorted(set(x))
        }).reset_index()
        
        bidder_agg.columns = [
            'Bidder Name', 'Bidder City', 'Bidder State',
            'Total Auctions', 'Wins', 'Bid Count', 'Last Active Date', 'Recent Participation'
        ]
        
        bidder_agg['Active'] = bidder_agg['Recent Participation'].apply(lambda x: 'Yes' if x else 'No')
        bidder_agg['Last Active Date'] = bidder_agg['Last Active Date'].dt.strftime('%Y-%m-%d')
        bidder_agg['Win Rate (%)'] = bidder_agg.apply(
            lambda row: f"{(row['Wins'] / row['Total Auctions'] * 100) if row['Total Auctions'] > 0 else 0:.2f}",
            axis=1
        )
        bidder_agg['Distance (km)'] = bidder_agg['Bidder City'].apply(
            lambda city: calculate_distance(loc_coords, city_coords.get(city, (20.5937, 78.9629)))
        )
        bidder_agg['Avg Bid Price'] = bidder_agg['Lowest Price'].round(2)
        bidder_agg['Remarks'] = bidder_agg.apply(
            lambda row: f"Initiators: {', '.join(row['Initiator'])}; Products: {', '.join(row['Product Name'])}",
            axis=1
        )
        
        result = bidder_agg[[
            'Bidder Name', 'Bidder City', 'Bidder State', 'Win Rate (%)',
            'Distance (km)', 'Avg Bid Price', 'Remarks', 'Last Active Date', 'Active'
        ]].to_dict('records')
        
        logger.info(f"Aggregated {len(result)} bidders for {product} in {location}")
        return {"bidders": result}
    except Exception as e:
        logger.error(f"Error processing: {e}")
        return {"error": f"Error processing data: {str(e)}"}

# Create table
def create_bidder_table(bidders, sort_by='Distance (km)'):
    if not bidders:
        return html.P("No bidders found.", className="text-red-600")
    
    bidders_df = pd.DataFrame(bidders)
    if sort_by == 'Win Rate (%)':
        bidders_df['Win Rate Sort'] = bidders_df['Win Rate (%)'].apply(lambda x: float(x))
        bidders_df = bidders_df.sort_values('Win Rate Sort', ascending=False)
    else:
        bidders_df = bidders_df.sort_values(sort_by, ascending=True)
    bidders = bidders_df.drop(columns=['Win Rate Sort'] if 'Win Rate Sort' in bidders_df.columns else []).to_dict('records')
    
    columns = ['Bidder Name', 'Bidder City', 'Bidder State', 'Win Rate (%)', 'Distance (km)', 'Avg Bid Price', 'Remarks', 'Last Active Date', 'Active']
    return html.Table([
        html.Thead(html.Tr([html.Th(col, className="border p-2 bg-gray-100") for col in columns])),
        html.Tbody([
            html.Tr([
                html.Td(bidder[col], className="border p-2") for col in columns
            ]) for bidder in bidders
        ])
    ], className="w-full border-collapse")

# Dash app
app = Dash(__name__)
server = app.server

# Load data
try:
    df = load_data()
    known_locations = df['Location'].unique().tolist() if not df.empty else []
    known_products = sorted(df['Normalized Product'].unique().tolist())
    products = ['Any Sugar'] + known_products
except Exception as e:
    df = pd.DataFrame()
    known_locations = []
    known_products = []
    products = ['Any Sugar']
    logger.error(f"Failed to load data: {e}")

# Layout
app.layout = html.Div(className="p-4", children=[
    html.H1("Sugar Procurement Chatbot", className="text-2xl font-bold mb-4"),
    dcc.Dropdown(
        id='product-dropdown',
        options=[{'label': p, 'value': p} for p in products],
        value='Any Sugar',
        className="w-full p-2 mb-2 border"
    ),
    dcc.Input(
        id='location-input',
        type='text',
        placeholder='Enter location (e.g., Anand)',
        className="w-full p-2 mb-2 border"
    ),
    dcc.Dropdown(
        id='sort-dropdown',
        options=[
            {'label': 'Distance', 'value': 'Distance (km)'},
            {'label': 'Win Rate', 'value': 'Win Rate (%)'},
            {'label': 'Price', 'value': 'Avg Bid Price'}
        ],
        value='Distance (km)',
        className="w-full p-2 mb-2 border"
    ),
    html.Button("Submit", id="submit-btn", n_clicks=0, className="p-2 bg-blue-500 text-white"),
    dcc.Download(id="download-excel"),
    html.Button("Download Excel", id="download-btn", n_clicks=0, className="p-2 bg-green-500 text-white mt-2"),
    dcc.Loading(id="loading", type="circle", children=html.Div(id='output', className="mt-4")),
    dcc.Store(id='data-store')
])

# Callback
@app.callback(
    [Output('output', 'children'), Output('data-store', 'data'), Output('download-excel', 'data')],
    [Input('submit-btn', 'n_clicks'), Input('sort-dropdown', 'value'), Input('download-btn', 'n_clicks')],
    [State('product-dropdown', 'value'), State('location-input', 'value'), State('data-store', 'data')]
)
def update_output(submit_clicks, sort_by, download_clicks, product, location, stored_data):
    ctx = dash.callback_context
    if not ctx.triggered:
        return [html.P("Enter a product and location."), None, None]
    
    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if triggered_id == 'download-btn':
        if stored_data and 'bidders' in stored_data:
            df = pd.DataFrame(stored_data['bidders'])
            output = BytesIO()
            df.to_excel(output, index=False)
            output.seek(0)
            return [
                create_bidder_table(stored_data['bidders'], sort_by),
                stored_data,
                dcc.send_bytes(output.getvalue(), f"bidders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
            ]
        return [html.P("No data to download.", className="text-red-600"), stored_data, None]
    
    if triggered_id in ['submit-btn', 'sort-dropdown']:
        if not product or not location:
            return [html.P("Please select a product and enter a location.", className="text-red-600"), None, None]
        
        product, location = parse_prompt(f"{product} for {location}", known_locations, known_products)
        if not product or not location:
            return [html.P("Invalid product or location.", className="text-red-600"), None, None]
        
        result = process_requirement(df, product, location)
        if "error" in result:
            return [html.P(result["error"], className="text-red-600"), None, None]
        
        bidders = result["bidders"]
        stored_data = {'product': product, 'location': location, 'bidders': bidders}
        return [create_bidder_table(bidders, sort_by), stored_data, None]

if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)