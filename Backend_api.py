"""
Compawnion Backend API - User Authentication & Account System
Provides endpoints for user registration, login, and session management
"""

from flask import Flask, request, jsonify, session
from flask_cors import CORS
import sqlite3
import hashlib
import secrets
from datetime import datetime, timedelta
import os

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(32))

# Configure CORS properly for GitHub Pages
CORS(app, 
     resources={r"/api/*": {
         "origins": [
             "https://jrcaulkins.github.io",
             "http://localhost:*",
             "http://127.0.0.1:*",
             "http://localhost:8888"
         ],
         "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
         "allow_headers": ["Content-Type", "Authorization"],
         "supports_credentials": True,
         "max_age": 3600
     }})

# Database configuration
DB_FILE = 'users.db'

def get_db_connection():
    """Create a database connection"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize the database with required tables"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create users table with password field
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_login TIMESTAMP,
        login_count INTEGER DEFAULT 0,
        is_active BOOLEAN DEFAULT 1
    )
    ''')
    
    # Create sessions table for tracking user sessions
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_sessions (
        session_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        session_token TEXT UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        expires_at TIMESTAMP NOT NULL,
        ip_address TEXT,
        user_agent TEXT,
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    )
    ''')
    
    # Create activity_log table (if not exists)
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
    
    conn.commit()
    conn.close()
    print("âœ… Database initialized")

def hash_password(password):
    """Hash a password using SHA-256 with salt"""
    salt = secrets.token_hex(16)
    pwd_hash = hashlib.sha256((password + salt).encode()).hexdigest()
    return f"{salt}${pwd_hash}"

def verify_password(password, password_hash):
    """Verify a password against its hash"""
    try:
        salt, pwd_hash = password_hash.split('$')
        return hashlib.sha256((password + salt).encode()).hexdigest() == pwd_hash
    except:
        return False

def create_session_token():
    """Generate a secure session token"""
    return secrets.token_urlsafe(32)

# ==================== API ENDPOINTS ====================

@app.route('/api/register', methods=['POST', 'OPTIONS'])
def register():
    """Register a new user account"""
    if request.method == 'OPTIONS':
        return '', 204
    
    data = request.json
    
    # Validate input
    if not data.get('username') or not data.get('email') or not data.get('password'):
        return jsonify({'error': 'Username, email, and password are required'}), 400
    
    username = data['username'].strip()
    email = data['email'].strip().lower()
    password = data['password']
    
    # Validate password strength
    if len(password) < 8:
        return jsonify({'error': 'Password must be at least 8 characters long'}), 400
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if username or email already exists
        cursor.execute('SELECT user_id FROM users WHERE username = ? OR email = ?', 
                      (username, email))
        if cursor.fetchone():
            return jsonify({'error': 'Username or email already exists'}), 409
        
        # Hash password and create user
        password_hash = hash_password(password)
        cursor.execute('''
            INSERT INTO users (username, email, password_hash, created_at)
            VALUES (?, ?, ?, ?)
        ''', (username, email, password_hash, datetime.now()))
        
        user_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return jsonify({
            'message': 'User registered successfully',
            'user_id': user_id,
            'username': username
        }), 201
        
    except sqlite3.IntegrityError as e:
        return jsonify({'error': 'Registration failed: ' + str(e)}), 500

@app.route('/api/login', methods=['POST', 'OPTIONS'])
def login():
    """User login endpoint"""
    if request.method == 'OPTIONS':
        return '', 204
    
    data = request.json
    
    if not data.get('username') or not data.get('password'):
        return jsonify({'error': 'Username and password are required'}), 400
    
    username = data['username'].strip()
    password = data['password']
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Find user by username or email
        cursor.execute('''
            SELECT user_id, username, email, password_hash, is_active, login_count
            FROM users 
            WHERE (username = ? OR email = ?) AND is_active = 1
        ''', (username, username))
        
        user = cursor.fetchone()
        
        if not user or not verify_password(password, user['password_hash']):
            return jsonify({'error': 'Invalid username or password'}), 401
        
        # Update last login and login count
        cursor.execute('''
            UPDATE users 
            SET last_login = ?, login_count = login_count + 1
            WHERE user_id = ?
        ''', (datetime.now(), user['user_id']))
        
        # Create session token
        session_token = create_session_token()
        expires_at = datetime.now() + timedelta(days=7)
        
        cursor.execute('''
            INSERT INTO user_sessions 
            (user_id, session_token, expires_at, ip_address, user_agent)
            VALUES (?, ?, ?, ?, ?)
        ''', (user['user_id'], session_token, expires_at, 
              request.remote_addr, request.headers.get('User-Agent')))
        
        conn.commit()
        conn.close()
        
        # Store in Flask session
        session['user_id'] = user['user_id']
        session['username'] = user['username']
        session['session_token'] = session_token
        
        return jsonify({
            'message': 'Login successful',
            'user': {
                'user_id': user['user_id'],
                'username': user['username'],
                'email': user['email'],
                'login_count': user['login_count'] + 1
            },
            'session_token': session_token
        }), 200
        
    except Exception as e:
        return jsonify({'error': 'Login failed: ' + str(e)}), 500

@app.route('/api/logout', methods=['POST', 'OPTIONS'])
def logout():
    """User logout endpoint"""
    if request.method == 'OPTIONS':
        return '', 204
    
    session_token = request.headers.get('Authorization') or session.get('session_token')
    
    if session_token:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM user_sessions WHERE session_token = ?', 
                          (session_token,))
            conn.commit()
            conn.close()
        except:
            pass
    
    session.clear()
    return jsonify({'message': 'Logged out successfully'}), 200

@app.route('/api/user/profile', methods=['GET', 'OPTIONS'])
def get_profile():
    """Get current user's profile"""
    if request.method == 'OPTIONS':
        return '', 204
    
    session_token = request.headers.get('Authorization') or session.get('session_token')
    
    if not session_token:
        return jsonify({'error': 'Not authenticated'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verify session
        cursor.execute('''
            SELECT u.user_id, u.username, u.email, u.created_at, u.last_login, u.login_count
            FROM users u
            JOIN user_sessions s ON u.user_id = s.user_id
            WHERE s.session_token = ? AND s.expires_at > ? AND u.is_active = 1
        ''', (session_token, datetime.now()))
        
        user = cursor.fetchone()
        
        if not user:
            return jsonify({'error': 'Invalid or expired session'}), 401
        
        # Get user statistics
        cursor.execute('''
            SELECT 
                COUNT(*) as total_activities,
                SUM(duration_minutes) as total_minutes,
                SUM(distance_km) as total_distance,
                SUM(steps) as total_steps
            FROM activity_log
            WHERE user_id = ?
        ''', (user['user_id'],))
        
        stats = cursor.fetchone()
        
        conn.close()
        
        return jsonify({
            'user': {
                'user_id': user['user_id'],
                'username': user['username'],
                'email': user['email'],
                'created_at': user['created_at'],
                'last_login': user['last_login'],
                'login_count': user['login_count']
            },
            'statistics': {
                'total_activities': stats['total_activities'] or 0,
                'total_minutes': stats['total_minutes'] or 0,
                'total_distance_km': stats['total_distance'] or 0,
                'total_steps': stats['total_steps'] or 0
            }
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/user/last-login', methods=['GET', 'OPTIONS'])
def get_last_login():
    """Get user's last login information"""
    if request.method == 'OPTIONS':
        return '', 204
    
    session_token = request.headers.get('Authorization') or session.get('session_token')
    
    if not session_token:
        return jsonify({'error': 'Not authenticated'}), 401
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT u.last_login, u.login_count
            FROM users u
            JOIN user_sessions s ON u.user_id = s.user_id
            WHERE s.session_token = ? AND s.expires_at > ?
        ''', (session_token, datetime.now()))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return jsonify({'error': 'Invalid session'}), 401
        
        return jsonify({
            'last_login': result['last_login'],
            'login_count': result['login_count']
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/health', methods=['GET', 'OPTIONS'])
def health_check():
    """Health check endpoint"""
    if request.method == 'OPTIONS':
        return '', 204
    
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    }), 200

# ==================== INITIALIZE & RUN ====================

if __name__ == '__main__':
    print("=" * 60)
    print("ðŸ• Compawnion Backend API Server")
    print("=" * 60)
    
    # Initialize database
    init_db()
    
    print("\nðŸ“‹ Available Endpoints:")
    print("  POST   /api/register       - Register new user")
    print("  POST   /api/login          - User login")
    print("  POST   /api/logout         - User logout")
    print("  GET    /api/user/profile   - Get user profile")
    print("  GET    /api/user/last-login - Get last login info")
    print("  GET    /api/health         - Health check")
    
    print("\nðŸš€ Starting server on http://localhost:5000")
    print("=" * 60)
    
    # Run the server
    app.run(debug=True, host='0.0.0.0', port=5000)
