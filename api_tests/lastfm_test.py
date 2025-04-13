import requests
import json

# Replace with your own API key
LASTFM_API_KEY = "c297e8c817537bb040134da565336e3c"
LASTFM_USER = "snigdhasv"  # User whose recent tracks to monitor

def test_lastfm_recent_tracks():
    """Test fetching recent tracks from Last.fm"""
    url = "http://ws.audioscrobbler.com/2.0/"
    params = {
        'method': 'user.getrecenttracks',
        'user': LASTFM_USER,
        'api_key': LASTFM_API_KEY,
        'format': 'json',
        'limit': 5
    }
    
    try:
        print("Requesting data from Last.fm API...")
        response = requests.get(url, params=params)
        response.raise_for_status()
        tracks = response.json()['recenttracks']['track']
        
        print(f"Successfully retrieved {len(tracks)} tracks from Last.fm!")
        print("\nSample track data:")
        sample_track = tracks[0]
        print(f"Track: {sample_track['name']}")
        print(f"Artist: {sample_track['artist']['#text']}")
        print(f"Album: {sample_track['album']['#text'] if 'album' in sample_track else 'N/A'}")
        
        # Print full JSON for the first track (formatted)
        print("\nComplete track data for first track:")
        print(json.dumps(sample_track, indent=2))
        
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"Response status code: {e.response.status_code}")
            print(f"Response body: {e.response.text}")
        return False

if __name__ == "__main__":
    test_lastfm_recent_tracks()