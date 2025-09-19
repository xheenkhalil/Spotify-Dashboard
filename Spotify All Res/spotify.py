import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from retry import retry
import requests.exceptions

# Replace 'your_client_id' and 'your_client_secret' with your Spotify API credentials
client_id = 'ae6ac4690e104864ab71b02d06a54563'
client_secret = '71277adf953747d880a875b5bb79598f'

# Replace 'your_dataset.csv' with the actual path to your dataset file
file_path = 'spotify_2023.csv'

# Load the dataset into a DataFrame with the appropriate encoding
df = pd.read_csv(file_path, encoding='ISO-8859-1')  # or 'ISO-8859-1' if 'latin1' doesn't work

# Set up Spotify API authentication
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Function to get the track details and extract the image URL with retry mechanism and increased timeout
@retry(tries=3, delay=2, backoff=2, exceptions=(requests.exceptions.ReadTimeout,))
def get_track_details(track_name, artist_name):
    # Search for the track
    results = sp.search(q=f"track:{track_name} artist:{artist_name}", type='track', limit=1)

    # Check if there are any results
    if results['tracks']['items']:
        track = results['tracks']['items'][0]
        # Get the album cover image URL
        image_url = track['album']['images'][0]['url']
        return image_url
    else:
        return None

# Add a new column 'image_url' to your dataset
df['image_url'] = df.apply(lambda row: get_track_details(row['track_name'], row['artist(s)_name']), axis=1)

# Save the updated DataFrame to a new CSV file with the added column
output_file_path = 'spotify_dataset.csv'  # Replace with your desired output file path
df.to_csv(output_file_path, index=False)
