import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize Spotify client
try:
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=os.getenv('SPOTIPY_CLIENT_ID'),
        client_secret=os.getenv('SPOTIPY_CLIENT_SECRET')
    ))
    
    # Test with a popular track (Ed Sheeran - Shape of You)
    track_id = '7qiZfU4dY1lWllzX7mPBI3'
    
    print("Testing Spotify API connection...")
    print("="*40)
    
    # 1. First test - Basic track info
    print("\n1. Testing track information...")
    track = sp.track(track_id)
    print(f"✅ Success! Track name: '{track['name']}' by {track['artists'][0]['name']}")
    
    # 2. Second test - Audio features
    print("\n2. Testing audio features...")
    features = sp.audio_features([track_id])[0]
    print(f"✅ Success! Audio features obtained:")
    print(f"   Danceability: {features['danceability']:.2f}")
    print(f"   Energy: {features['energy']:.2f}")
    print(f"   Tempo: {features['tempo']:.1f} BPM")
    
    # 3. Third test - Verify authentication
    print("\n3. Verifying authentication scope...")
    token_info = sp.auth_manager.get_access_token(as_dict=False)
    print(f"✅ Authentication successful! Token received: {token_info[:15]}...")
    
    print("\nAll tests passed! 🎉 Your Spotify API setup is working correctly.")
    
except spotipy.SpotifyException as e:
    print("\n❌ Spotify API Error:")
    print(f"HTTP Status: {e.http_status}")
    print(f"Error Code: {e.code}")
    print(f"Message: {e.msg}")
    
except Exception as e:
    print("\n❌ Unexpected Error:")
    print(str(e))