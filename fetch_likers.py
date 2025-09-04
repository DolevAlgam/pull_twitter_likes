#!/usr/bin/env python3
"""
Twitter Likes Fetcher - Retrieves all users who liked a specific tweet
Uses OAuth 1.0a authentication and stores data in SQLite with CSV export
"""

import os
import sys
import time
import sqlite3
import requests
import csv
import json
import random
import signal
import threading
from typing import Optional, List, Tuple, Dict
from requests_oauthlib import OAuth1
from datetime import datetime

# Always flush prints immediately so logs are visible during waits
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

# Configuration from environment variables
CONSUMER_KEY = os.environ.get("CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("CONSUMER_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET")
TWEET_ID = os.environ.get("TWEET_ID")
DB_PATH = os.environ.get("DB_PATH", "state.db")
OUT_DIR = os.environ.get("OUT_DIR", ".")
EXPORT_EVERY_SECS = int(os.environ.get("EXPORT_EVERY_SECS", "300"))  # 5 minutes
S3_URI = os.environ.get("S3_URI")  # Optional S3 upload
API_BASE = "https://api.twitter.com/2"
EXPORT_MODE = os.environ.get("EXPORT_MODE", "final").lower()  # 'final' or 'periodic'

# Validate required environment variables
if not all([CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET, TWEET_ID]):
    print("❌ Missing required environment variables:", file=sys.stderr)
    print("   CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET, TWEET_ID", file=sys.stderr)
    sys.exit(1)

# Create output directory
os.makedirs(OUT_DIR, exist_ok=True)

# Global stop flag for graceful shutdown
stop_flag = False

def handle_stop(signum, frame):
    """Handle SIGTERM and SIGINT for graceful shutdown"""
    global stop_flag
    print(f"\n🛑 Received signal {signum}, setting stop flag...")
    stop_flag = True

# Register signal handlers
signal.signal(signal.SIGTERM, handle_stop)
signal.signal(signal.SIGINT, handle_stop)

class TwitterLikesFetcher:
    def __init__(self):
        self.db_path = DB_PATH
        self.out_dir = OUT_DIR
        self.tweet_id = TWEET_ID
        self.export_interval = EXPORT_EVERY_SECS
        
        # Setup OAuth 1.0a authentication
        self.auth = OAuth1(
            CONSUMER_KEY,
            CONSUMER_SECRET,
            ACCESS_TOKEN,
            ACCESS_TOKEN_SECRET
        )
        
        # Setup requests session
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'twitter-likes-fetcher/1.0'
        })
        
        # Initialize database
        self.init_database()
        
        # Start periodic export thread only if explicitly requested
        if EXPORT_MODE == "periodic":
            self.export_thread = threading.Thread(target=self.periodic_export, daemon=True)
            self.export_thread.start()
        
        print(f"🚀 Twitter Likes Fetcher initialized")
        print(f"📱 Tweet ID: {self.tweet_id}")
        print(f"🌐 Tweet URL: https://x.com/dolevalgam/status/{self.tweet_id}")
        print(f"💾 Database: {self.db_path}")
        print(f"📁 Output: {self.out_dir}")
        print()

    def init_database(self):
        """Initialize SQLite database with required tables"""
        self.conn = sqlite3.connect(self.db_path, timeout=30)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        
        # Create tables
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS likers (
                tweet_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                username TEXT,
                name TEXT,
                verified INTEGER,
                created_at TEXT,
                description TEXT,
                profile_url TEXT,
                public_metrics TEXT,
                PRIMARY KEY(tweet_id, user_id)
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS state (
                tweet_id TEXT PRIMARY KEY,
                next_token TEXT,
                done INTEGER DEFAULT 0,
                last_request_time INTEGER,
                total_users_found INTEGER DEFAULT 0,
                last_export_time INTEGER DEFAULT 0
            )
        """)
        
        self.conn.commit()
        print("✅ Database initialized")

    def get_state(self, tweet_id: str) -> Tuple[Optional[str], bool, int, int]:
        """Get current state for a tweet"""
        print(f"🔍 VERBOSE: Getting state for tweet {tweet_id}")
        row = self.conn.execute(
            "SELECT next_token, done, total_users_found, last_export_time FROM state WHERE tweet_id=?",
            (tweet_id,)
        ).fetchone()
        
        if row:
            print(f"🔍 VERBOSE: Found existing state: next_token={row[0]}, done={bool(row[1])}, total_users={row[2]}, last_export={row[3]}")
            return row[0], bool(row[1]), row[2], row[3]
        
        # Initialize state for new tweet
        print(f"🔍 VERBOSE: No existing state found, initializing new state")
        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO state(tweet_id,next_token,done,total_users_found,last_export_time) VALUES(?,NULL,0,0,0)",
                (tweet_id,)
            )
        return None, False, 0, 0

    def save_state(self, tweet_id: str, next_token: Optional[str], done: bool, total_users: int):
        """Save current state for a tweet"""
        current_time = int(time.time())
        print(f"🔍 VERBOSE: Saving state: next_token={next_token}, done={done}, total_users={total_users}")
        with self.conn:
            self.conn.execute(
                "UPDATE state SET next_token=?, done=?, total_users_found=?, last_request_time=? WHERE tweet_id=?",
                (next_token, int(done), total_users, current_time, tweet_id)
            )
        print(f"🔍 VERBOSE: State saved successfully")

    def update_export_time(self, tweet_id: str):
        """Update last export time"""
        current_time = int(time.time())
        with self.conn:
            self.conn.execute(
                "UPDATE state SET last_export_time=? WHERE tweet_id=?",
                (current_time, tweet_id)
            )

    def insert_users(self, tweet_id: str, users: List[Dict]):
        """Insert users into database, ignoring duplicates"""
        with self.conn:
            for user in users:
                # Construct profile URL
                username = user.get('username', '')
                profile_url = f"https://x.com/{username}" if username else ""
                
                # Store public_metrics as JSON string
                public_metrics = json.dumps(user.get('public_metrics', {}))
                
                self.conn.execute("""
                    INSERT OR IGNORE INTO likers
                    (tweet_id, user_id, username, name, verified, created_at, description, profile_url, public_metrics)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    tweet_id,
                    user.get('id'),
                    user.get('username'),
                    user.get('name'),
                    int(bool(user.get('verified'))),
                    user.get('created_at'),
                    user.get('description', ''),
                    profile_url,
                    public_metrics
                ))

    def pace_requests(self, response):
        """Handle rate limiting based on response headers"""
        print(f"🔍 VERBOSE: Response status: {response.status_code}")
        print(f"🔍 VERBOSE: Rate limit headers: remaining={response.headers.get('x-rate-limit-remaining', 'N/A')}, reset={response.headers.get('x-rate-limit-reset', 'N/A')}")
        
        try:
            remaining = int(response.headers.get("x-rate-limit-remaining", "1"))
            reset = int(response.headers.get("x-rate-limit-reset", "0"))
        except (ValueError, TypeError):
            remaining, reset = 1, 0
        
        print(f"🔍 VERBOSE: Parsed remaining={remaining}, reset={reset}")
        
        # In QUICK_TEST mode, do not sleep here to keep tests fast; just log
        if os.environ.get("QUICK_TEST") == "true":
            print(f"⏳ QUICK_TEST: Skipping waits in pace_requests (remaining={remaining}, reset={reset})")
            return
        else:
            if remaining <= 1 and reset:
                # Calculate wait time until rate limit resets (interruptible)
                current_time = int(time.time())
                wait_time = max(0, reset - current_time) + 2  # Add 2 second buffer
                print(f"⏳ Rate limit reached. Waiting {wait_time} seconds until reset...")
                for remaining_sec in range(wait_time, 0, -1):
                    if stop_flag:
                        print("🛑 Stop flag set during wait; breaking wait")
                        break
                    if remaining_sec % 10 == 0 or remaining_sec <= 5:
                        print(f"⏳ {remaining_sec} seconds remaining...")
                    time.sleep(1)
            elif remaining <= 5:
                # If we're close to the limit, add a small delay
                delay = random.uniform(1, 3)
                print(f"⏳ Approaching rate limit ({remaining} remaining). Waiting {delay:.1f}s...")
                time.sleep(delay)

    def backoff_sleep(self, attempt: int):
        """Exponential backoff for retries"""
        delay = min(300, (2 ** attempt)) + random.uniform(0, 1.2)
        print(f"⏳ Backoff attempt {attempt + 1}, waiting {delay:.1f}s...")
        time.sleep(delay)

    def fetch_page(self, tweet_id: str, next_token: Optional[str]) -> Dict:
        """Fetch a page of users who liked the tweet"""
        url = f"{API_BASE}/tweets/{tweet_id}/liking_users"
        
        # Use smaller max_results for testing if TEST_MODE is set
        if os.environ.get("TEST_MODE") == "true":
            max_results = 2  # Very small for quick testing
        elif os.environ.get("QUICK_TEST") == "true":
            max_results = 2  # Quick test mode
        else:
            max_results = 100  # Production mode
        
        params = {
            'user.fields': 'id,name,username,verified,created_at,description,public_metrics',
            'max_results': max_results
        }
        
        if next_token:
            params['pagination_token'] = next_token
        
        for attempt in range(7):  # Max 7 retry attempts
            try:
                print(f"📡 Fetching page (attempt {attempt + 1})...")
                print(f"🔍 VERBOSE: URL: {url}")
                print(f"🔍 VERBOSE: Params: {params}")
                response = self.session.get(url, params=params, timeout=30)
                
                print(f"🔍 VERBOSE: Response received, status: {response.status_code}")
                
                # Handle rate limiting
                self.pace_requests(response)
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"✅ Successfully fetched page")
                    print(f"🔍 VERBOSE: Response data keys: {list(data.keys())}")
                    if 'data' in data:
                        print(f"🔍 VERBOSE: Found {len(data['data'])} users in response")
                    if 'meta' in data:
                        print(f"🔍 VERBOSE: Meta: {data['meta']}")
                    return data
                
                elif response.status_code == 429:
                    # Rate limited - use server reset time if available
                    print(f"🚨 429 RATE LIMITED! Attempt {attempt + 1}")
                    print(f"🔍 VERBOSE: Got 429, checking reset time...")
                    reset = int(response.headers.get("x-rate-limit-reset", "0") or "0")
                    print(f"🔍 VERBOSE: Reset timestamp from header: {reset}")
                    
                    if reset:
                        current_time = int(time.time())
                        delay = max(5, reset - current_time + 2)
                        print(f"⏳ Rate limited by server. Reset at {reset}, current time {current_time}")
                        print(f"⏳ Need to wait {delay} seconds until reset...")
                        
                        # For testing, use shorter wait times
                        if os.environ.get("QUICK_TEST") == "true":
                            test_delay = 5
                            print(f"⏳ TESTING: Waiting only {test_delay} seconds instead of {delay}")
                            delay = test_delay
                        
                        # Countdown timer with stop flag checking
                        for remaining in range(delay, 0, -1):
                            if stop_flag:
                                print(f"🛑 Stop flag set, breaking wait")
                                return None
                            if remaining % 10 == 0 or remaining <= 5:
                                print(f"⏳ {remaining} seconds remaining...")
                            time.sleep(1)
                        
                        if stop_flag:
                            print(f"🛑 Stopped during wait")
                            return None
                        
                        print(f"✅ Wait complete, retrying...")
                    else:
                        print(f"⏳ No reset time in header, using exponential backoff...")
                        self.backoff_sleep(attempt)
                    continue
                
                elif 500 <= response.status_code < 600:
                    # Server error - retry with backoff
                    print(f"🔍 VERBOSE: Server error {response.status_code}, retrying...")
                    self.backoff_sleep(attempt)
                    continue
                
                else:
                    # Client error (4xx) - likely fatal; return control so caller can checkpoint and export
                    print(f"❌ HTTP {response.status_code}: {response.text}", file=sys.stderr)
                    return None
                    
            except requests.RequestException as e:
                print(f"❌ Network error: {e}")
                self.backoff_sleep(attempt)
                continue
        
        print(f"❌ Too many retries for tweet {tweet_id}", file=sys.stderr)
        return None

    def export_csv(self, tweet_id: str) -> str:
        """Export users to CSV file"""
        return self.export_csv_with_connection(tweet_id, self.conn)

    def export_csv_with_connection(self, tweet_id: str, conn: sqlite3.Connection) -> str:
        """Export users to CSV file using provided connection"""
        # Append epoch time to filename to avoid overwriting on repeated runs
        epoch_suffix = str(int(time.time())) if EXPORT_MODE == "final" else "current"
        csv_path = os.path.join(self.out_dir, f"{tweet_id}_likers_{epoch_suffix}.csv")
        
        # Get all users for this tweet
        cursor = conn.execute("""
            SELECT user_id, username, name, verified, created_at, description, profile_url, public_metrics
            FROM likers 
            WHERE tweet_id=? 
            ORDER BY user_id
        """, (tweet_id,))
        
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "tweet_id", "user_id", "username", "name", "verified", 
                "created_at", "description", "profile_url", "public_metrics"
            ])
            
            for row in cursor:
                writer.writerow([tweet_id] + list(row))
        
        # Update export time (only if using main connection)
        if conn == self.conn:
            self.update_export_time(tweet_id)
        
        return csv_path

    def periodic_export(self):
        """Periodically export CSV files"""
        while not stop_flag:
            try:
                time.sleep(self.export_interval)
                if not stop_flag:
                    # Create a new database connection for this thread
                    conn = sqlite3.connect(self.db_path, timeout=30)
                    csv_path = self.export_csv_with_connection(self.tweet_id, conn)
                    conn.close()
                    print(f"📄 Exported CSV: {csv_path}")
            except Exception as e:
                print(f"❌ Export error: {e}")

    def estimate_completion_time(self, total_users: int, current_users: int) -> str:
        """Estimate completion time based on current progress"""
        if current_users == 0:
            return "Unknown"
        
        # Assume 15 minutes per request (conservative estimate)
        requests_needed = (total_users - current_users) // 100 + 1
        hours_remaining = (requests_needed * 15) / 60
        
        if hours_remaining < 1:
            return f"{int(hours_remaining * 60)} minutes"
        else:
            return f"{hours_remaining:.1f} hours"

    def run(self):
        """Main execution loop"""
        print(f"🎯 Starting to fetch likes for tweet {self.tweet_id}")
        
        next_token, done, total_users, last_export = self.get_state(self.tweet_id)
        
        if done:
            print("✅ Tweet already completed!")
            csv_path = self.export_csv(self.tweet_id)
            print(f"📄 Final CSV: {csv_path}")
            return
        
        page_count = 0
        max_pages = 3 if os.environ.get("QUICK_TEST") == "true" else 999999
        
        while not done and not stop_flag and page_count < max_pages:
            page_count += 1
            print(f"\n📄 Processing page {page_count}...")
            print(f"🔍 VERBOSE: Current next_token: {next_token}")
            print(f"🔍 VERBOSE: Total users found so far: {total_users}")
            
            # Fetch the page
            data = self.fetch_page(self.tweet_id, next_token)
            
            if data is None:
                print("❌ Failed to fetch page (likely rate limited or stopped)")
                print("💾 Saving current state before stopping...")
                self.save_state(self.tweet_id, next_token, False, total_users)
                break
            
            # Process users
            users = data.get('data', [])
            if users:
                self.insert_users(self.tweet_id, users)
                total_users += len(users)
                print(f"👥 Found {len(users)} users (total: {total_users})")
            else:
                print("📝 No users found in this page")
            
            # Update pagination
            meta = data.get('meta', {})
            next_token = meta.get('next_token')
            done = not bool(next_token)
            
            # Save state
            self.save_state(self.tweet_id, next_token, done, total_users)
            
            # Show progress
            if not done:
                estimated_time = self.estimate_completion_time(800, total_users)  # Assuming ~800 likes
                print(f"⏱️  Estimated completion: {estimated_time}")
            
            # Small delay between requests to be respectful
            if not done and not stop_flag:
                if os.environ.get("QUICK_TEST") == "true":
                    time.sleep(1)  # 1 second in test mode
                else:
                    time.sleep(2)  # 2 seconds in production
        
        if stop_flag:
            print("🛑 Stopped by user signal")
        elif page_count >= max_pages:
            print(f"🛑 Reached max pages limit ({max_pages})")
        else:
            print("🎉 Completed fetching all likes!")
        
        # Final export only in final or periodic modes, and only if rows exist
        if EXPORT_MODE in ("final", "periodic"):
            rows = self.conn.execute(
                "SELECT COUNT(*) FROM likers WHERE tweet_id=?",
                (self.tweet_id,)
            ).fetchone()[0]
            if rows > 0:
                csv_path = self.export_csv(self.tweet_id)
                print(f"📄 Final CSV exported: {csv_path}")
            else:
                print("📄 Skipping CSV export (no rows)")
        else:
            print("📄 Skipping CSV export due to EXPORT_MODE")
        
        # Show summary
        final_count = self.conn.execute(
            "SELECT COUNT(*) FROM likers WHERE tweet_id=?", (self.tweet_id,)
        ).fetchone()[0]
        print(f"📊 Total users found: {final_count}")

def main():
    """Main entry point"""
    print("🐦 Twitter Likes Fetcher")
    print("=" * 50)
    
    try:
        fetcher = TwitterLikesFetcher()
        fetcher.run()
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        print("👋 Goodbye!")

if __name__ == "__main__":
    main()
