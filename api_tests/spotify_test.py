import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json

# Replace with your Spotify API credentials
SPOTIFY_CLIENT_ID = "ae1aa8a3d64e40e09598ea17a1537f2f"
SPOTIFY_CLIENT_SECRET = "f500311fc12b4d33b53272427b6b00de"

def get_track_data_without_audio_features():
    """Get track data using only the track object (no audio features)"""
    try:
        print("Initializing Spotify client...")
        client_credentials_manager = SpotifyClientCredentials(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET
        )
        spotify = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
        
        # Search for a track
        artist_name = "Daft Punk"
        track_name = "Get Lucky"
        print(f"\nSearching for track: '{track_name}' by '{artist_name}'")
        
        query = f"artist:{artist_name} track:{track_name}"
        results = spotify.search(q=query, type='track', limit=1)
        
        if not results['tracks']['items']:
            print("No tracks found!")
            return False
        
        # Extract track data from search result
        track = results['tracks']['items'][0]
        
        # Get additional track details
        track_details = spotify.track(track['id'])
        
        # Extract useful information
        track_data = {
            'title': track['name'],
            'artist': track['artists'][0]['name'],
            'album': track['album']['name'],
            'release_date': track['album']['release_date'],
            'popularity': track['popularity'],  # 0-100 score of popularity
            'duration_ms': track['duration_ms'],
            'explicit': track['explicit'],
            'track_number': track['track_number'],
            'spotify_id': track['id'],
            'spotify_url': track['external_urls']['spotify'],
            'preview_url': track['preview_url'],
            
            # Some audio info available from the track object
            'available_markets': len(track['available_markets']),
            'disc_number': track['disc_number'],
            
            # Album info
            'album_type': track['album']['album_type'],
            'total_tracks': track['album']['total_tracks'],
            'album_image': track['album']['images'][0]['url'] if track['album']['images'] else None
        }
        
        print("\nSuccessfully extracted track data!")
        print(json.dumps(track_data, indent=2))
        
        return track_data
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    get_track_data_without_audio_features()