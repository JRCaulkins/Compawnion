import sqlite3
import json
from datetime import datetime, timedelta
import os

# Use a persistent database file instead of in-memory
DB_FILE = 'users.db'
connection = sqlite3.connect(DB_FILE)
cursor = connection.cursor()

# Create users table
cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')

# Create activity_log table for tracking walks and activities
cursor.execute('''
CREATE TABLE IF NOT EXISTS activity_log (
    activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    activity_date DATE NOT NULL,
    activity_type TEXT NOT NULL,
    duration_minutes INTEGER,
    distance_km REAL,
    steps INTEGER,
    location_lat REAL,
    location_lng REAL,
    park_name TEXT,
    weather_condition TEXT,
    temperature_f INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
)
''')

# Create routes table for common walking routes
cursor.execute('''
CREATE TABLE IF NOT EXISTS routes (
    route_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    route_name TEXT,
    start_lat REAL,
    start_lng REAL,
    end_lat REAL,
    end_lng REAL,
    distance_km REAL,
    avg_duration_minutes INTEGER,
    times_used INTEGER DEFAULT 1,
    last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
)
''')

# Create user_preferences table for ML features
cursor.execute('''
CREATE TABLE IF NOT EXISTS user_preferences (
    user_id INTEGER PRIMARY KEY,
    preferred_time_of_day TEXT,
    preferred_activity_type TEXT,
    avg_walk_duration INTEGER,
    favorite_park TEXT,
    dog_size TEXT,
    dog_energy_level TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
)
''')

# Create aggregated_stats table for ML training features
cursor.execute('''
CREATE TABLE IF NOT EXISTS aggregated_stats (
    user_id INTEGER PRIMARY KEY,
    total_steps INTEGER DEFAULT 0,
    total_distance_km REAL DEFAULT 0,
    total_walk_time_minutes INTEGER DEFAULT 0,
    total_activities INTEGER DEFAULT 0,
    avg_duration_per_walk INTEGER DEFAULT 0,
    most_common_activity TEXT,
    most_visited_park TEXT,
    preferred_weather TEXT,
    activity_frequency_per_week REAL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
)
''')

print("✅ Database tables created successfully")

# Function to insert sample data for testing
def insert_sample_data():
    """Insert sample user data for testing the ML pipeline"""
    
    # Sample users
    sample_users = [
        ('john_doe', 'john@example.com'),
        ('jane_smith', 'jane@example.com'),
        ('bob_wilson', 'bob@example.com')
    ]
    
    cursor.executemany(
        'INSERT OR IGNORE INTO users (username, email) VALUES (?, ?)',
        sample_users
    )
    
    # Sample activity data
    user_ids = [1, 2, 3]
    activity_types = ['walk', 'run', 'park_visit', 'training']
    parks = ['Sycamore Dog Park', 'Warner Park Dog Exercise Area', 'Quann Dog Park']
    weather = ['sunny', 'cloudy', 'rainy']
    
    sample_activities = []
    for user_id in user_ids:
        for i in range(30):  # 30 days of activity
            date = datetime.now() - timedelta(days=i)
            sample_activities.append((
                user_id,
                date.date(),
                activity_types[i % len(activity_types)],
                30 + (i % 60),  # duration 30-90 mins
                2.0 + (i % 5),  # distance 2-7 km
                3000 + (i % 7000),  # steps 3000-10000
                43.07 + (i % 10) * 0.01,  # lat
                -89.40 + (i % 10) * 0.01,  # lng
                parks[i % len(parks)],
                weather[i % len(weather)],
                50 + (i % 30)  # temp 50-80F
            ))
    
    cursor.executemany('''
        INSERT INTO activity_log 
        (user_id, activity_date, activity_type, duration_minutes, distance_km, 
         steps, location_lat, location_lng, park_name, weather_condition, temperature_f)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', sample_activities)
    
    print(f"✅ Inserted {len(sample_activities)} sample activities")

# Function to update aggregated stats for ML
def update_aggregated_stats():
    """Calculate and update aggregated statistics for each user"""
    
    cursor.execute('SELECT DISTINCT user_id FROM activity_log')
    user_ids = [row[0] for row in cursor.fetchall()]
    
    for user_id in user_ids:
        # Calculate aggregate statistics
        cursor.execute('''
            SELECT 
                COALESCE(SUM(steps), 0) as total_steps,
                COALESCE(SUM(distance_km), 0) as total_distance,
                COALESCE(SUM(duration_minutes), 0) as total_time,
                COUNT(*) as total_activities,
                COALESCE(AVG(duration_minutes), 0) as avg_duration
            FROM activity_log
            WHERE user_id = ?
        ''', (user_id,))
        
        stats = cursor.fetchone()
        
        # Find most common activity type
        cursor.execute('''
            SELECT activity_type, COUNT(*) as count
            FROM activity_log
            WHERE user_id = ?
            GROUP BY activity_type
            ORDER BY count DESC
            LIMIT 1
        ''', (user_id,))
        
        most_common = cursor.fetchone()
        most_common_activity = most_common[0] if most_common else 'walk'
        
        # Find most visited park
        cursor.execute('''
            SELECT park_name, COUNT(*) as count
            FROM activity_log
            WHERE user_id = ? AND park_name IS NOT NULL
            GROUP BY park_name
            ORDER BY count DESC
            LIMIT 1
        ''', (user_id,))
        
        most_park = cursor.fetchone()
        most_visited_park = most_park[0] if most_park else None
        
        # Find preferred weather
        cursor.execute('''
            SELECT weather_condition, COUNT(*) as count
            FROM activity_log
            WHERE user_id = ? AND weather_condition IS NOT NULL
            GROUP BY weather_condition
            ORDER BY count DESC
            LIMIT 1
        ''', (user_id,))
        
        weather_pref = cursor.fetchone()
        preferred_weather = weather_pref[0] if weather_pref else 'sunny'
        
        # Calculate activity frequency per week
        cursor.execute('''
            SELECT MIN(activity_date), MAX(activity_date), COUNT(*)
            FROM activity_log
            WHERE user_id = ?
        ''', (user_id,))
        
        date_range = cursor.fetchone()
        if date_range[0] and date_range[1]:
            start_date = datetime.strptime(date_range[0], '%Y-%m-%d')
            end_date = datetime.strptime(date_range[1], '%Y-%m-%d')
            weeks = max((end_date - start_date).days / 7, 1)
            frequency = date_range[2] / weeks
        else:
            frequency = 0
        
        # Insert or update aggregated stats
        cursor.execute('''
            INSERT OR REPLACE INTO aggregated_stats 
            (user_id, total_steps, total_distance_km, total_walk_time_minutes, 
             total_activities, avg_duration_per_walk, most_common_activity, 
             most_visited_park, preferred_weather, activity_frequency_per_week, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_id,
            stats[0],  # total_steps
            stats[1],  # total_distance
            stats[2],  # total_time
            stats[3],  # total_activities
            int(stats[4]),  # avg_duration
            most_common_activity,
            most_visited_park,
            preferred_weather,
            frequency,
            datetime.now()
        ))
    
    print(f"✅ Updated aggregated stats for {len(user_ids)} users")

# Function to export data for scikit-learn
def export_for_ml(filename='ml_training_data.json'):
    """Export user data in a format ready for scikit-learn"""
    
    cursor.execute('''
        SELECT 
            u.user_id,
            u.username,
            a.total_steps,
            a.total_distance_km,
            a.total_walk_time_minutes,
            a.total_activities,
            a.avg_duration_per_walk,
            a.most_common_activity,
            a.most_visited_park,
            a.preferred_weather,
            a.activity_frequency_per_week
        FROM users u
        LEFT JOIN aggregated_stats a ON u.user_id = a.user_id
    ''')
    
    columns = [
        'user_id', 'username', 'total_steps', 'total_distance_km', 
        'total_walk_time_minutes', 'total_activities', 'avg_duration_per_walk',
        'most_common_activity', 'most_visited_park', 'preferred_weather',
        'activity_frequency_per_week'
    ]
    
    data = []
    for row in cursor.fetchall():
        data.append(dict(zip(columns, row)))
    
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"✅ Exported {len(data)} user records to {filename}")
    return data

# Main execution
if __name__ == '__main__':
    print("🐕 Compawnion User Activity Database")
    print("=" * 50)
    
    # Check if this is a fresh database
    cursor.execute("SELECT COUNT(*) FROM users")
    user_count = cursor.fetchone()[0]
    
    if user_count == 0:
        print("📊 No existing data found. Inserting sample data...")
        insert_sample_data()
    
    # Update aggregated statistics
    print("\n📈 Updating aggregated statistics...")
    update_aggregated_stats()
    
    # Export data for ML
    print("\n🤖 Exporting data for machine learning...")
    ml_data = export_for_ml()
    
    # Display summary
    print("\n📊 Database Summary:")
    cursor.execute("SELECT COUNT(*) FROM users")
    print(f"   • Total users: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM activity_log")
    print(f"   • Total activities logged: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM aggregated_stats")
    print(f"   • Users with aggregated stats: {cursor.fetchone()[0]}")
    
    print("\n✨ Database operations complete!")

# Commit changes to the database
connection.commit()

# Close the connection when done
connection.close()
