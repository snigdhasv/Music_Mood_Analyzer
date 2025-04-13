import dash
from dash import dcc, html, Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
import mysql.connector
import pandas as pd
from collections import defaultdict
import json
import time
from datetime import datetime, timedelta

# MySQL configuration
MYSQL_CONFIG = {
    'host': '172.24.144.1',
    'database': 'music_mood_analyzer',
    'user': 'wsluser',
    'password': 'wslpass',
    'port': 3306,
    'autocommit': True,
    'connect_timeout': 30,
    'pool_name': 'mood_pool',
    'pool_size': 5,
    'pool_reset_session': True
}

# Initialize the Dash app
app = dash.Dash(__name__, title="Music Mood Analyzer Dashboard")

# Define app layout
app.layout = html.Div([
    html.Header([
        html.H1("Music Mood Analyzer", className="dashboard-title"),
        html.P("Real-time analytics of music mood patterns", className="dashboard-subtitle")
    ], className="dashboard-header"),
    
    html.Div([
        html.Div([
            html.Div([
                html.H3("View Settings", className="section-title"),
                dcc.RadioItems(
                    id='time-range',
                    options=[
                        {'label': 'Last 30 minutes', 'value': 30},
                        {'label': 'Last hour', 'value': 60},
                        {'label': 'Last 3 hours', 'value': 180},
                        {'label': 'Last 24 hours', 'value': 1440},
                        {'label': 'All time', 'value': 0}
                    ],
                    value=60,
                    className="radio-items",
                    inputClassName="radio-input",
                    labelClassName="radio-label"
                ),
                html.Button('Refresh Data', id='refresh-button', className="refresh-button"),
                html.Div(id='last-update-time', className="update-time")
            ], className="control-panel"),
            
            html.Div([
                html.H3("Currently Playing", className="card-title"),
                html.Div(id='current-song-info', className="current-song-content")
            ], className="card current-song-card"),
        ], className="sidebar"),
        
        html.Div([
            html.Div([
                html.Div([
                    html.H3("Real-Time Audio Features by Mood", className="card-title"),
                    dcc.Graph(id='comparison-graph', className="graph-content")
                ], className="card features-card"),
                
                html.Div([
                    html.H3("Mood Distribution", className="card-title"),
                    dcc.Graph(id='mood-distribution', className="graph-content")
                ], className="card distribution-card"),
            ], className="card-row"),
            
            html.Div([
                html.Div([
                    html.H3("User Feedback by Mood", className="card-title"),
                    dcc.Graph(id='feedback-by-mood', className="graph-content")
                ], className="card feedback-card"),
                
                html.Div([
                    html.H3("Playlist Recommendations", className="card-title"),
                    dcc.Graph(id='playlist-recommendations', className="graph-content")
                ], className="card recommendations-card"),
            ], className="card-row"),
        ], className="main-content"),
        
        # Hidden div for storing the data
        html.Div(id='stored-data', style={'display': 'none'})
    ], className="dashboard-content"),
    
    # Interval component for auto-refresh
    dcc.Interval(
        id='interval-component',
        interval=30*1000,  # refresh every 30 seconds
        n_intervals=0
    ),
    
    html.Footer([
        html.P("© 2025 Music Mood Analyzer", className="footer-text")
    ], className="dashboard-footer")
])

# Add CSS to the app
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Music Mood Analyzer Dashboard</title>
        {%metas%}
        {%favicon%}
        {%css%}
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
        <style>
            :root {
                /* Pastel color palette */
                --primary: #a5b4fc;
                --primary-light: #c7d2fe;
                --primary-dark: #818cf8;
                --secondary: #f0abfc;
                --neutral-50: #f8fafc;
                --neutral-100: #f1f5f9;
                --neutral-200: #e2e8f0;
                --neutral-300: #cbd5e1;
                --neutral-400: #94a3b8;
                --neutral-500: #64748b;
                --neutral-600: #475569;
                --neutral-700: #334155;
                --neutral-800: #1e293b;
                --neutral-900: #0f172a;
                
                /* Mood colors - muted pastels */
                --mood-happy: #fde68a;
                --mood-energetic: #fca5a5;
                --mood-calm: #a5b4fc;
                --mood-sad: #c4b5fd;
                --mood-dark: #a1a1aa;
                --mood-neutral: #d4d4d8;
                
                /* Spacing */
                --space-xs: 4px;
                --space-sm: 8px;
                --space-md: 16px;
                --space-lg: 24px;
                --space-xl: 32px;
                
                /* Shadows */
                --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.05);
                --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
                --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
                
                /* Border radius */
                --radius-sm: 4px;
                --radius-md: 8px;
                --radius-lg: 16px;
            }
            
            * {
                box-sizing: border-box;
                margin: 0;
                padding: 0;
            }
            
            body {
                font-family: 'Outfit', sans-serif;
                font-weight: 400;
                line-height: 1.6;
                background-color: var(--neutral-100);
                color: var(--neutral-800);
                margin: 0;
                padding: 0;
                min-height: 100vh;
                display: flex;
                flex-direction: column;
            }
            
            /* Header styles */
            .dashboard-header {
                background: linear-gradient(to right, var(--primary), var(--secondary));
                color: white;
                padding: var(--space-lg) var(--space-xl);
                text-align: center;
                box-shadow: var(--shadow-md);
            }
            
            .dashboard-title {
                font-weight: 700;
                font-size: 2.2rem;
                margin: 0;
                letter-spacing: -0.02em;
            }
            
            .dashboard-subtitle {
                font-weight: 400;
                opacity: 0.9;
                margin-top: var(--space-xs);
                font-size: 1.1rem;
            }
            
            /* Main content layout */
            .dashboard-content {
                display: flex;
                padding: var(--space-lg);
                gap: var(--space-lg);
                flex: 1;
                max-width: 1600px;
                margin: 0 auto;
                width: 100%;
            }
            
            .sidebar {
                flex: 0 0 300px;
                display: flex;
                flex-direction: column;
                gap: var(--space-lg);
            }
            
            .main-content {
                flex: 1;
                display: flex;
                flex-direction: column;
                gap: var(--space-lg);
            }
            
            .card-row {
                display: flex;
                gap: var(--space-lg);
                width: 100%;
            }
            
            /* Card styles */
            .card {
                background-color: white;
                border-radius: var(--radius-lg);
                box-shadow: var(--shadow-md);
                padding: var(--space-lg);
                transition: transform 0.2s, box-shadow 0.2s;
                flex: 1;
                overflow: hidden;
                display: flex;
                flex-direction: column;
            }
            
            .card:hover {
                transform: translateY(-2px);
                box-shadow: var(--shadow-lg);
            }
            
            .card-title {
                font-size: 1.1rem;
                font-weight: 600;
                color: var(--neutral-700);
                margin-bottom: var(--space-md);
                padding-bottom: var(--space-sm);
                border-bottom: 1px solid var(--neutral-200);
            }
            
            .graph-content {
                flex: 1;
                min-height: 250px;
            }
            
            /* Control panel styles */
            .control-panel {
                background-color: white;
                border-radius: var(--radius-lg);
                box-shadow: var(--shadow-md);
                padding: var(--space-lg);
            }
            
            .section-title {
                font-size: 1.1rem;
                font-weight: 600;
                color: var(--neutral-700);
                margin-bottom: var(--space-md);
            }
            
            .radio-items {
                display: flex;
                flex-direction: column;
                gap: var(--space-sm);
                margin-bottom: var(--space-md);
            }
            
            .radio-label {
                display: flex;
                align-items: center;
                cursor: pointer;
                padding: var(--space-xs) 0;
            }
            
            .radio-input {
                margin-right: var(--space-sm);
                accent-color: var(--primary);
            }
            
            .refresh-button {
                width: 100%;
                background-color: var(--primary);
                color: white;
                border: none;
                border-radius: var(--radius-md);
                padding: var(--space-sm) var(--space-md);
                font-weight: 500;
                cursor: pointer;
                transition: background-color 0.2s;
                font-family: 'Outfit', sans-serif;
                font-size: 0.9rem;
            }
            
            .refresh-button:hover {
                background-color: var(--primary-dark);
            }
            
            .update-time {
                margin-top: var(--space-md);
                font-size: 0.85rem;
                color: var(--neutral-500);
                text-align: center;
            }
            
            /* Current song styles */
            .current-song-card {
                flex: 1;
            }
            
            .current-song-content {
                display: flex;
                flex-direction: column;
                gap: var(--space-sm);
                padding: var(--space-md);
                background-color: var(--neutral-50);
                border-radius: var(--radius-md);
            }
            
            .song-title {
                font-weight: 600;
                font-size: 1.1rem;
                color: var(--neutral-800);
                margin: 0;
            }
            
            .song-artist {
                font-weight: 400;
                font-size: 0.95rem;
                color: var(--neutral-600);
                margin: 0;
            }
            
            .song-album {
                font-size: 0.85rem;
                color: var(--neutral-500);
                margin: 0;
            }
            
            .song-mood {
                display: inline-block;
                padding: var(--space-xs) var(--space-sm);
                border-radius: var(--radius-sm);
                font-size: 0.8rem;
                font-weight: 600;
                margin-top: var(--space-sm);
                text-transform: uppercase;
            }
            
            /* Footer styles */
            .dashboard-footer {
                background-color: var(--neutral-800);
                color: var(--neutral-300);
                text-align: center;
                padding: var(--space-lg);
                font-size: 0.85rem;
            }
            
            /* Responsive adjustments */
            @media (max-width: 1200px) {
                .dashboard-content {
                    flex-direction: column;
                }
                
                .sidebar {
                    flex-direction: row;
                    flex: none;
                    width: 100%;
                }
                
                .control-panel, .current-song-card {
                    flex: 1;
                }
            }
            
            @media (max-width: 768px) {
                .card-row {
                    flex-direction: column;
                }
                
                .sidebar {
                    flex-direction: column;
                }
                
                .dashboard-content {
                    padding: var(--space-md);
                }
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# Helper function to connect to MySQL
def get_db_connection():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    return conn

# Helper function to get data from MySQL
def get_data(minutes=60):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Calculate time range
        if minutes > 0:
            start_time = int(time.time()) - (minutes * 60)
            time_condition = f"WHERE timestamp >= {start_time}"
        else:
            time_condition = ""
        
        # Get songs data
        cursor.execute(f"""
            SELECT * FROM songs {time_condition}
            ORDER BY timestamp DESC
        """)
        songs = cursor.fetchall()
        
        # Get feedback data
        if minutes > 0:
            start_time_str = (datetime.now() - timedelta(minutes=minutes)).strftime('%Y-%m-%d %H:%M:%S')
            feedback_condition = f"WHERE timestamp >= '{start_time_str}'"
        else:
            feedback_condition = ""
            
        cursor.execute(f"""
            SELECT * FROM feedback {feedback_condition}
            ORDER BY timestamp DESC
        """)
        feedback = cursor.fetchall()
        
        # Get playlist recommendations
        cursor.execute(f"""
            SELECT * FROM playlist_recommendations {time_condition}
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        recommendations = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {
            'songs': songs,
            'feedback': feedback,
            'recommendations': recommendations,
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        # Generate mock data for demonstration
        current_time = int(time.time())
        
        # Mock songs data
        mock_songs = [
            {
                'id': 1,
                'track': 'Mock Song 1',
                'artist': 'Mock Artist 1',
                'album': 'Mock Album 1',
                'mood': 'happy',
                'valence': 0.8,
                'energy': 0.7,
                'acousticness': 0.3,
                'danceability': 0.6,
                'tempo': 120,
                'timestamp': current_time - 300,
                'image': 'https://via.placeholder.com/100'
            },
            {
                'id': 2,
                'track': 'Mock Song 2',
                'artist': 'Mock Artist 2',
                'album': 'Mock Album 2',
                'mood': 'calm',
                'valence': 0.4,
                'energy': 0.3,
                'acousticness': 0.8,
                'danceability': 0.3,
                'tempo': 80,
                'timestamp': current_time - 600,
                'image': 'https://via.placeholder.com/100'
            },
            {
                'id': 3,
                'track': 'Mock Song 3',
                'artist': 'Mock Artist 3',
                'album': 'Mock Album 3',
                'mood': 'energetic',
                'valence': 0.7,
                'energy': 0.9,
                'acousticness': 0.2,
                'danceability': 0.8,
                'tempo': 130,
                'timestamp': current_time - 900,
                'image': 'https://via.placeholder.com/100'
            }
        ]
        
        # Mock feedback data
        mock_feedback = [
            {
                'id': 1,
                'track_id': 'Mock Artist 1 - Mock Song 1',
                'mood': 'happy',
                'action': 'like',
                'feedback_value': 0.9,
                'timestamp': current_time - 250,
                'user_id': 'user1'
            },
            {
                'id': 2,
                'track_id': 'Mock Artist 2 - Mock Song 2',
                'mood': 'calm',
                'action': 'listen_full',
                'feedback_value': 0.7,
                'timestamp': current_time - 550,
                'user_id': 'user1'
            },
            {
                'id': 3,
                'track_id': 'Mock Artist 3 - Mock Song 3',
                'mood': 'energetic',
                'action': 'add_to_playlist',
                'feedback_value': 0.8,
                'timestamp': current_time - 850,
                'user_id': 'user1'
            }
        ]
        
        # Mock recommendations data
        mock_recommendations = [
            {
                'id': 1,
                'recommendations': json.dumps({
                    'happy': 0.4,
                    'energetic': 0.3,
                    'calm': 0.2,
                    'sad': 0.05,
                    'dark': 0.05
                }),
                'timestamp': current_time - 100
            }
        ]
        
        return {
            'songs': mock_songs,
            'feedback': mock_feedback,
            'recommendations': mock_recommendations,
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' (MOCK DATA)'
        }

# Callback to refresh data
@callback(
    Output('stored-data', 'children'),
    Output('last-update-time', 'children'),
    Input('refresh-button', 'n_clicks'),
    Input('interval-component', 'n_intervals'),
    Input('time-range', 'value')
)
def refresh_data(n_clicks, n_intervals, time_range):
    data = get_data(time_range)
    last_update = f"Last updated: {data['last_update']}"
    
    # Convert datetime objects to strings for JSON serialization
    def convert_datetime(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: convert_datetime(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_datetime(item) for item in obj]
        else:
            return obj
    
    # Convert all datetime objects in the data
    serializable_data = convert_datetime(data)
    
    return json.dumps(serializable_data), last_update

# Callback for current song info
@callback(
    Output('current-song-info', 'children'),
    Input('stored-data', 'children')
)
def update_current_song(json_data):
    if not json_data:
        return html.Div("No data available")
    
    data = json.loads(json_data)
    songs = data.get('songs', [])
    
    if not songs:
        return html.Div("No songs available")
    
    # Get the most recent song
    current_song = songs[0]
    
    # Define mood colors - using pastel colors
    mood_colors = {
        'happy': 'var(--mood-happy)',
        'energetic': 'var(--mood-energetic)',
        'calm': 'var(--mood-calm)',
        'sad': 'var(--mood-sad)',
        'dark': 'var(--mood-dark)',
        'neutral': 'var(--mood-neutral)'
    }
    
    mood = current_song.get('mood', 'neutral')
    mood_color = mood_colors.get(mood, 'var(--mood-neutral)')
    
    # Removed album art as requested
    return html.Div([
        html.P(current_song.get('track', 'Unknown'), className="song-title"),
        html.P(current_song.get('artist', 'Unknown Artist'), className="song-artist"),
        html.P(f"Album: {current_song.get('album', 'Unknown')}", className="song-album"),
        html.Div(
            mood.capitalize(),
            className="song-mood",
            style={'backgroundColor': mood_color, 'color': 'var(--neutral-800)'}
        )
    ], className="song-details")

# Callback for mood distribution chart
@callback(
    Output('mood-distribution', 'figure'),
    Input('stored-data', 'children')
)
def update_mood_distribution(json_data):
    if not json_data:
        return go.Figure()
    
    data = json.loads(json_data)
    songs = data.get('songs', [])
    
    if not songs:
        return go.Figure()
    
    # Count songs by mood
    mood_counts = defaultdict(int)
    for song in songs:
        mood = song.get('mood', 'unknown')
        mood_counts[mood] += 1
    
    # Create dataframe for plotting
    df = pd.DataFrame([
        {'mood': mood, 'count': count}
        for mood, count in mood_counts.items()
    ])
    
    # Define mood colors - using pastel colors
    colors = {
        'happy': '#fde68a',       # Pastel yellow
        'energetic': '#fca5a5',   # Pastel red
        'calm': '#a5b4fc',        # Pastel blue
        'sad': '#c4b5fd',         # Pastel purple
        'dark': '#a1a1aa',        # Pastel gray
        'neutral': '#d4d4d8',     # Light gray
        'unknown': '#e2e8f0'      # Very light gray
    }
    
    # Create pie chart
    fig = px.pie(
        df, 
        values='count', 
        names='mood',
        color='mood',
        color_discrete_map=colors,
        hole=0.4
    )
    
    fig.update_traces(
        textposition='inside', 
        textinfo='percent+label',
        marker=dict(line=dict(color='white', width=2))
    )
    
    fig.update_layout(
        margin=dict(t=10, b=10, l=10, r=10),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.15,
            xanchor="center",
            x=0.5
        ),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(
            family="Outfit, sans-serif",
            color="#1e293b"
        )
    )
    
    return fig

# Callback for feedback by mood chart
@callback(
    Output('feedback-by-mood', 'figure'),
    Input('stored-data', 'children')
)
def update_feedback_chart(json_data):
    if not json_data:
        return go.Figure()
    
    data = json.loads(json_data)
    feedback = data.get('feedback', [])
    
    if not feedback:
        return go.Figure()
    
    # Group feedback by mood
    mood_feedback = defaultdict(list)
    for record in feedback:
        mood = record.get('mood', 'unknown')
        value = record.get('feedback_value', 0)
        mood_feedback[mood].append(value)
    
    # Calculate average feedback for each mood
    mood_avg = {}
    for mood, values in mood_feedback.items():
        if values:
            mood_avg[mood] = sum(values) / len(values)
    
    # Create dataframe for plotting
    df = pd.DataFrame([
        {'mood': mood, 'average_feedback': avg}
        for mood, avg in mood_avg.items()
    ])
    
    # Define mood colors - using pastel colors
    colors = {
        'happy': '#fde68a',       # Pastel yellow
        'energetic': '#fca5a5',   # Pastel red
        'calm': '#a5b4fc',        # Pastel blue
        'sad': '#c4b5fd',         # Pastel purple
        'dark': '#a1a1aa',        # Pastel gray
        'neutral': '#d4d4d8',     # Light gray
        'unknown': '#e2e8f0'      # Very light gray
    }
    
    # Create bar chart
    fig = px.bar(
        df,
        x='mood',
        y='average_feedback',
        color='mood',
        color_discrete_map=colors,
        text_auto='.2f'
    )
    
    fig.update_traces(
        textposition='outside',
        marker_line_color='white',
        marker_line_width=1.5,
        opacity=0.8
    )
    
    fig.update_layout(
        xaxis_title=None,
        yaxis_title='Average Feedback',
        yaxis=dict(range=[-1, 1], gridcolor='#e2e8f0', zeroline=True, zerolinecolor='#94a3b8'),
        margin=dict(t=10, b=40, l=10, r=10),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        legend_title=None,
        showlegend=False,
        font=dict(
            family="Outfit, sans-serif",
            color="#1e293b"
        )
    )
    
    return fig

# Callback for playlist recommendations chart
@callback(
    Output('playlist-recommendations', 'figure'),
    Input('stored-data', 'children')
)
def update_recommendations_chart(json_data):
    if not json_data:
        return go.Figure()
    
    data = json.loads(json_data)
    recommendations = data.get('recommendations', [])
    
    if not recommendations:
        return go.Figure()
    
    # Get latest recommendation
    latest = recommendations[0]
    recommendations_str = latest.get('recommendations', '{}')
    
    try:
        mood_weights = json.loads(recommendations_str)
    except:
        mood_weights = {}
    
    if not mood_weights:
        return go.Figure()
    
    # Create dataframe for plotting
    df = pd.DataFrame([
        {'mood': mood, 'weight': weight}
        for mood, weight in mood_weights.items()
    ])
    
    # Define mood colors - using pastel colors
    colors = {
        'happy': '#fde68a',       # Pastel yellow
        'energetic': '#fca5a5',   # Pastel red
        'calm': '#a5b4fc',        # Pastel blue
        'sad': '#c4b5fd',         # Pastel purple
        'dark': '#a1a1aa',        # Pastel gray
        'neutral': '#d4d4d8',     # Light gray
        'unknown': '#e2e8f0'      # Very light gray
    }
    
    # Create horizontal bar chart
    fig = px.bar(
        df,
        y='mood',
        x='weight',
        color='mood',
        color_discrete_map=colors,
        orientation='h',
        text_auto='.0%'
    )
    
    fig.update_traces(
        textposition='outside',
        marker_line_color='white',
        marker_line_width=1.5,
        opacity=0.8
    )
    
    fig.update_layout(
        xaxis_title='Recommended Proportion',
        yaxis_title=None,
        xaxis=dict(range=[0, max(0.05, max(df['weight']) * 1.1)], gridcolor='#e2e8f0'),
        margin=dict(t=10, b=10, l=10, r=80),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        legend_title=None,
        showlegend=False,
        font=dict(
            family="Outfit, sans-serif",
            color="#1e293b"
        )
    )
    
    return fig

# Callback for comparison graph
@callback(
    Output('comparison-graph', 'figure'),
    Input('stored-data', 'children')
)
def update_comparison_graph(json_data):
    if not json_data:
        return go.Figure()
    
    data = json.loads(json_data)
    songs = data.get('songs', [])
    
    if not songs:
        return go.Figure()
    
    # Create a plot showing mean audio features
    features = ['valence', 'energy', 'acousticness', 'danceability']
    
    # Group by mood
    mood_features = defaultdict(lambda: defaultdict(list))
    
    for song in songs:
        mood = song.get('mood', 'unknown')
        for feature in features:
            value = song.get(feature, 0)
            mood_features[mood][feature].append(value)
    
    # Calculate averages
    mood_avg = {}
    for mood, feat_values in mood_features.items():
        mood_avg[mood] = {feat: sum(values) / len(values) if values else 0 
                          for feat, values in feat_values.items()}
    
    # Create radar chart
    fig = go.Figure()
    
    # Define mood colors - using pastel colors
    colors = {
        'happy': '#fde68a',       # Pastel yellow
        'energetic': '#fca5a5',   # Pastel red
        'calm': '#a5b4fc',        # Pastel blue
        'sad': '#c4b5fd',         # Pastel purple
        'dark': '#a1a1aa',        # Pastel gray
        'neutral': '#d4d4d8',     # Light gray
        'unknown': '#e2e8f0'      # Very light gray
    }
    
    for mood, avg_features in mood_avg.items():
        fig.add_trace(go.Scatterpolar(
            r=[avg_features.get(feat, 0) for feat in features] + [avg_features.get(features[0], 0)],
            theta=features + [features[0]],
            fill='toself',
            name=mood.capitalize(),
            line_color=colors.get(mood, '#e2e8f0'),
            line_width=2,
            opacity=0.7
        ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 1],
                linecolor='#cbd5e1',
                gridcolor='#e2e8f0'
            ),
            angularaxis=dict(
                linecolor='#cbd5e1',
                gridcolor='#e2e8f0'
            ),
            bgcolor='rgba(0,0,0,0)'
        ),
        margin=dict(t=10, b=40, l=10, r=10),
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(
            family="Outfit, sans-serif",
            color="#1e293b"
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5,
            font=dict(size=10)
        )
    )
    
    return fig

# Run the app
if __name__ == '__main__':
    app.run(debug=True, port=8051)