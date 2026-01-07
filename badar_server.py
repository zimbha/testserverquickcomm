#!/usr/bin/env python3
"""
BADAR AI - Complete Integrated Server
UDP Audio Communication + HTTP Registration API
Optimized for minimal latency and real-time communication
"""

import socket
import threading
import time
import sqlite3
import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, Set, Tuple
import json

from flask import Flask, request, jsonify
from flask_cors import CORS

# =================== CONFIGURATION ===================
UDP_HOST = "0.0.0.0"
UDP_PORT = 12345
API_HOST = "0.0.0.0"
API_PORT = 8001
DB_PATH = "badar_audio.db"
BUFFER_SIZE = 2048

# =================== LOGGING ===================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# =================== DATABASE ===================
class Database:
    """Unified database handler"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_db()
    
    def _init_db(self):
        """Initialize database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS devices (
                    device_id TEXT PRIMARY KEY,
                    organization_id INTEGER NOT NULL,
                    property_id INTEGER NOT NULL,
                    team_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    ip_address TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_team 
                ON devices(organization_id, property_id, team_id, status)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_last_seen 
                ON devices(last_seen)
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    packets_received INTEGER,
                    packets_relayed INTEGER,
                    active_sessions INTEGER
                )
            """)
            
            conn.commit()
            logger.info("✅ Database initialized")
    
    def register_device(self, device_id, org_id, property_id, team_id, user_id, ip, port):
        """Register or update device"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO devices 
                    (device_id, organization_id, property_id, team_id, user_id, 
                     ip_address, port, last_seen, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 'active')
                """, (device_id, org_id, property_id, team_id, user_id, ip, port))
                conn.commit()
    
    def update_last_seen(self, device_id):
        """Update device heartbeat"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    UPDATE devices 
                    SET last_seen = CURRENT_TIMESTAMP 
                    WHERE device_id = ?
                """, (device_id,))
                conn.commit()
    
    def get_team_members(self, org_id, property_id, team_id, exclude=None):
        """Get active team members"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                query = """
                    SELECT device_id, ip_address, port 
                    FROM devices 
                    WHERE organization_id = ? 
                      AND property_id = ? 
                      AND team_id = ? 
                      AND status = 'active'
                      AND datetime(last_seen) > datetime('now', '-60 seconds')
                """
                params = [org_id, property_id, team_id]
                
                if exclude:
                    query += " AND device_id != ?"
                    params.append(exclude)
                
                cursor = conn.execute(query, params)
                return cursor.fetchall()
    
    def get_device_info(self, device_id):
        """Get device details"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT organization_id, property_id, team_id, user_id
                    FROM devices 
                    WHERE device_id = ?
                """, (device_id,))
                return cursor.fetchone()
    
    def get_device_by_addr(self, ip, port):
        """Find device by IP and port"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT device_id, organization_id, property_id, team_id, user_id
                    FROM devices 
                    WHERE ip_address = ? AND port = ? AND status = 'active'
                """, (ip, port))
                return cursor.fetchone()
    
    def save_stats(self, packets_rx, packets_tx, active):
        """Save statistics snapshot"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO stats (packets_received, packets_relayed, active_sessions)
                    VALUES (?, ?, ?)
                """, (packets_rx, packets_tx, active))
                conn.commit()


# =================== UDP AUDIO RELAY ===================
class AudioRelay:
    """High-performance UDP audio relay"""
    
    def __init__(self, host, port, database):
        self.host = host
        self.port = port
        self.db = database
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2097152)  # 2MB
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2097152)  # 2MB
        
        # Session cache: {(ip, port): (device_id, org, prop, team, user)}
        self.sessions = {}
        self.sessions_lock = threading.Lock()
        
        # Statistics
        self.packets_received = 0
        self.packets_relayed = 0
        self.packets_dropped = 0
        self.last_heartbeat = {}
        
        self.running = False
    
    def start(self):
        """Start UDP relay"""
        self.sock.bind((self.host, self.port))
        self.running = True
        
        logger.info(f"🎵 UDP Audio Relay: {self.host}:{self.port}")
        
        # Start receive loop
        threading.Thread(target=self._receive_loop, daemon=True).start()
        
        # Start heartbeat updater
        threading.Thread(target=self._heartbeat_worker, daemon=True).start()
    
    def _receive_loop(self):
        """Main packet receiving loop - optimized for speed"""
        logger.info("📡 UDP receive loop started")
        
        while self.running:
            try:
                data, addr = self.sock.recvfrom(BUFFER_SIZE)
                self.packets_received += 1
                
                # Get or lookup session
                session = self._get_session(addr)
                
                if session:
                    device_id, org_id, prop_id, team_id, user_id = session
                    
                    # Track heartbeat
                    self.last_heartbeat[addr] = time.time()
                    
                    # Get team members and relay
                    targets = self.db.get_team_members(org_id, prop_id, team_id, device_id)
                    
                    # Relay to all team members
                    for _, target_ip, target_port in targets:
                        try:
                            self.sock.sendto(data, (target_ip, target_port))
                            self.packets_relayed += 1
                        except Exception as e:
                            self.packets_dropped += 1
                            logger.debug(f"Drop: {target_ip}:{target_port} - {e}")
                else:
                    # Unknown device - try to lookup in DB
                    device_info = self.db.get_device_by_addr(addr[0], addr[1])
                    if device_info:
                        self._cache_session(addr, device_info)
                        logger.info(f"✅ Session cached: {device_info[0]} from {addr[0]}")
                    else:
                        if self.packets_received % 100 == 0:  # Log occasionally
                            logger.warning(f"⚠️ Unregistered device: {addr[0]}:{addr[1]}")
                
            except Exception as e:
                if self.running:
                    logger.error(f"Receive error: {e}")
    
    def _get_session(self, addr):
        """Get cached session info"""
        with self.sessions_lock:
            return self.sessions.get(addr)
    
    def _cache_session(self, addr, device_info):
        """Cache session info"""
        with self.sessions_lock:
            self.sessions[addr] = device_info
    
    def _heartbeat_worker(self):
        """Update database heartbeats periodically"""
        while self.running:
            time.sleep(5)
            
            current_time = time.time()
            with self.sessions_lock:
                for addr, session_info in list(self.sessions.items()):
                    if addr in self.last_heartbeat:
                        # Update DB if device was active in last 10 seconds
                        if current_time - self.last_heartbeat[addr] < 10:
                            device_id = session_info[0]
                            self.db.update_last_seen(device_id)
    
    def get_stats(self):
        """Get current statistics"""
        with self.sessions_lock:
            active = len(self.sessions)
        
        return {
            'packets_received': self.packets_received,
            'packets_relayed': self.packets_relayed,
            'packets_dropped': self.packets_dropped,
            'active_sessions': active
        }
    
    def reset_stats(self):
        """Reset counters"""
        self.packets_received = 0
        self.packets_relayed = 0
        self.packets_dropped = 0
    
    def stop(self):
        """Stop relay"""
        self.running = False
        self.sock.close()


# =================== HTTP API ===================
app = Flask(__name__)
CORS(app)

# Global references (set by main)
db = None
relay = None

@app.route('/health', methods=['GET'])
def health_check():
    """Health check"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/register', methods=['POST'])
def register_device():
    """Register device for UDP communication"""
    try:
        data = request.json
        
        required = ['device_id', 'organization_id', 'property_id', 
                   'team_id', 'user_id', 'ip_address', 'port']
        
        for field in required:
            if field not in data:
                return jsonify({"error": f"Missing: {field}"}), 400
        
        db.register_device(
            data['device_id'],
            data['organization_id'],
            data['property_id'],
            data['team_id'],
            data['user_id'],
            data['ip_address'],
            data['port']
        )
        
        logger.info(f"📱 Registered: {data['device_id']} | Team: {data['team_id']}")
        
        return jsonify({
            "status": "success",
            "device_id": data['device_id']
        }), 200
        
    except Exception as e:
        logger.error(f"Registration error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/devices/<device_id>/heartbeat', methods=['POST'])
def heartbeat(device_id):
    """Update device heartbeat"""
    try:
        db.update_last_seen(device_id)
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/team/<int:team_id>', methods=['GET'])
def get_team(team_id):
    """Get team members"""
    try:
        org_id = int(request.args.get('org_id', 0))
        prop_id = int(request.args.get('property_id', 0))
        
        members = db.get_team_members(org_id, prop_id, team_id)
        
        return jsonify({
            "team_id": team_id,
            "count": len(members),
            "members": [{"device": m[0], "ip": m[1], "port": m[2]} for m in members]
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get system statistics"""
    try:
        stats = relay.get_stats()
        return jsonify(stats), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =================== MAIN ===================
def stats_reporter(relay, db):
    """Periodically report statistics"""
    while True:
        time.sleep(10)
        stats = relay.get_stats()
        
        logger.info(
            f"📊 Stats | Sessions: {stats['active_sessions']} | "
            f"RX: {stats['packets_received']} | "
            f"TX: {stats['packets_relayed']} | "
            f"Drop: {stats['packets_dropped']}"
        )
        
        # Save to database
        db.save_stats(
            stats['packets_received'],
            stats['packets_relayed'],
            stats['active_sessions']
        )
        
        # Reset counters
        relay.reset_stats()

def main():
    global db, relay
    
    print("\n╔═══════════════════════════════════════╗")
    print("║     BADAR AI INTEGRATED SERVER       ║")
    print("║  UDP Audio + HTTP Registration API   ║")
    print("╚═══════════════════════════════════════╝\n")
    
    # Initialize database
    db = Database(DB_PATH)
    logger.info(f"📁 Database: {DB_PATH}")
    
    # Start UDP relay
    relay = AudioRelay(UDP_HOST, UDP_PORT, db)
    relay.start()
    
    # Start statistics reporter
    threading.Thread(target=stats_reporter, args=(relay, db), daemon=True).start()
    
    # Start HTTP API
    logger.info(f"🌐 HTTP API: http://{API_HOST}:{API_PORT}")
    logger.info("✅ All systems ready\n")
    
    try:
        app.run(host=API_HOST, port=API_PORT, debug=False, threaded=True)
    except KeyboardInterrupt:
        logger.info("\n🛑 Shutting down...")
        relay.stop()

if __name__ == "__main__":
    main()
