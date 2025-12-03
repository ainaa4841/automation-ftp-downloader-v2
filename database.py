import psycopg2
from psycopg2 import sql
import logging
from typing import List, Dict, Any, Optional, Union
import threading
import json
import os
import time
from datetime import datetime

class ThreadSafeDB:
    """Lightweight DB access for threads with auto reconnect."""
    def __init__(self):
        import psycopg2
        self.conn = psycopg2.connect(
            host="localhost",
            database="ftp_db",
            user="ftp_user",
            password="123456",
            port=5432
        )
        self.lock = threading.Lock()

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.conn_params)
        except Exception as e:
            logging.error(f"DB connection failed: {e}")
            self.connection = None

    def execute(self, query, params=None, fetch=False):
        with self._lock:
            if self.connection is None:
                self.connect()
            if self.connection is None:
                return None
            try:
                with self.connection.cursor() as cur:
                    cur.execute(query, params)
                    if fetch:
                        result = cur.fetchall()
                        self.connection.commit()
                        return result
                    self.connection.commit()
                    return True
            except Exception as e:
                logging.error(f"DB query failed: {e}")
                try:
                    self.connection.rollback()
                except:
                    pass
                return None

    def log_download(self, username, station_id, filename, local_path, status, message):
        with self.lock:
            try:
                cur = self.conn.cursor()
                cur.execute("""
                    INSERT INTO download_history (username, station_id, filename, local_path, status, message)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (username, station_id, filename)
                    DO UPDATE SET
                        local_path = EXCLUDED.local_path,
                        status = EXCLUDED.status,
                        message = EXCLUDED.message,
                        created_at = CURRENT_TIMESTAMP
                """, (username, station_id, filename, local_path, status, message))
                self.conn.commit()
                cur.close()
            except Exception as e:
                print(f"[DB LOG ERROR] {e}")

class DatabaseManager:
    def __init__(self, host="localhost", database="ftp_db", user="ftp_user", password="123456", port=5432):
        self.connection_params = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port
        }
        self.connection = None
        self.connect()
        self.create_tables()
        

    # ===========================================================
    # Database Connection
    # ===========================================================
    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            logging.info("Database connected successfully")
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            self.connection = None

    def _ensure_connection(self):
        """Ensure DB connection is alive; reconnect if necessary."""
        try:
            if self.connection is None:
                self.connect()
            else:
                # simple lightweight check
                with self.connection.cursor() as cur:
                    cur.execute("SELECT 1")
            return self.connection is not None
        except Exception as e:
            logging.warning(f"DB connection lost, reconnecting: {e}")
            try:
                self.connect()
                return self.connection is not None
            except Exception as e2:
                logging.error(f"Reconnect failed: {e2}")
                self.connection = None
                return False

    def execute_query(self, query: str, params: Optional[tuple] = None, fetch: bool = False):
        """Executes SQL queries safely and reconnects if needed."""
        # Ensure connection
        if not self._ensure_connection():
            logging.error("Database connection not established.")
            return None

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                if fetch:
                    results = cursor.fetchall()
                    self.connection.commit()
                    return results
                self.connection.commit()
                return True
        except Exception as e:
            logging.error(f"Database query error: {e}")
            # rollback and try to reconnect once
            try:
                if self.connection:
                    self.connection.rollback()
            except Exception:
                pass
            # attempt reconnect one more time
            try:
                self.connect()
                with self.connection.cursor() as cursor:
                    cursor.execute(query, params)
                    if fetch:
                        results = cursor.fetchall()
                        self.connection.commit()
                        return results
                    self.connection.commit()
                    return True
            except Exception as e2:
                logging.error(f"Database query retry failed: {e2}")
                if self.connection:
                    try:
                        self.connection.rollback()
                    except Exception:
                        pass
                return None


    # ===========================================================
    # Create Tables
    # ===========================================================
    def create_tables(self):
        """Create tables using username as primary key instead of ID."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS servers (
                username VARCHAR(255) PRIMARY KEY,
                host VARCHAR(255) NOT NULL,
                port INTEGER DEFAULT 21,
                password VARCHAR(255) NOT NULL,
                remote_path VARCHAR(500),
                is_selected BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS stations (
                station_id VARCHAR(255) NOT NULL,
                username VARCHAR(255) REFERENCES servers(username) ON DELETE CASCADE,
                is_selected BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (station_id, username)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS app_settings (
                key VARCHAR(255) PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        ]

        for query in queries:
            self.execute_query(query)

    # ===========================================================
    # Server Management
    # ===========================================================
    def add_server(self, host: str, port: int, username: str, password: str, remote_path: str, is_selected: bool = False) -> bool:
        query = """
            INSERT INTO servers (host, port, username, password, remote_path, is_selected)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (username) DO UPDATE SET
                host = EXCLUDED.host,
                port = EXCLUDED.port,
                password = EXCLUDED.password,
                remote_path = EXCLUDED.remote_path,
                is_selected = EXCLUDED.is_selected
        """
        result = self.execute_query(query, (host, port, username, password, remote_path, is_selected))
        return result is True

    def get_servers(self) -> List[Dict[str, Any]]:
        query = "SELECT host, port, username, password, remote_path, is_selected FROM servers ORDER BY username"
        results = self.execute_query(query, fetch=True)
        if not results:
            return []
        
        # Convert to dictionary format for easier access
        servers = []
        for row in results:
            servers.append({
                'host': row[0],
                'port': row[1],
                'username': row[2],
                'password': row[3],
                'remote_path': row[4],
                'is_selected': row[5]
            })
        return servers

    def update_server(self, username: str, host: Optional[str] = None, port: Optional[int] = None, 
                     password: Optional[str] = None, remote_path: Optional[str] = None, 
                     is_selected: Optional[bool] = None) -> bool:
        query = "UPDATE servers SET "
        updates = []
        params = []

        if host is not None:
            updates.append("host = %s")
            params.append(host)
        if port is not None:
            updates.append("port = %s")
            params.append(port)
        if password is not None:
            updates.append("password = %s")
            params.append(password)
        if remote_path is not None:
            updates.append("remote_path = %s")
            params.append(remote_path)
        if is_selected is not None:
            updates.append("is_selected = %s")
            params.append(is_selected)

        if updates:
            query += ", ".join(updates) + " WHERE username = %s"
            params.append(username)
            result = self.execute_query(query, tuple(params))
            return result is True
        return False

    def delete_server(self, username: str) -> bool:
        query = "DELETE FROM servers WHERE username = %s"
        result = self.execute_query(query, (username,))
        return result is True

    # ===========================================================
    # Station Management
    # ===========================================================
    def get_stations_by_username(self, username):
        if self.connection is None:
            self.connect()
            
        if self.connection is None:
            raise ConnectionError("Database connection not initialized.")
        
        with self.connection.cursor() as cur:
            cur.execute("SELECT station_id FROM stations WHERE username = %s", (username,))
            rows = cur.fetchall()
            return [{"station_id": r[0]} for r in rows]
        
    def add_station(self, station_id: str, username: str, is_selected: bool = False) -> bool:
        query = """
            INSERT INTO stations (station_id, username, is_selected)
            VALUES (%s, %s, %s)
            ON CONFLICT (station_id, username) DO UPDATE SET
                is_selected = EXCLUDED.is_selected
        """
        result = self.execute_query(query, (station_id, username, is_selected))
        return result is True

    def get_stations(self, username: Optional[str] = None) -> List[Dict[str, Any]]:
        if username:
            query = "SELECT station_id, username, is_selected FROM stations WHERE username = %s ORDER BY station_id"
            results = self.execute_query(query, (username,), fetch=True)
        else:
            query = "SELECT station_id, username, is_selected FROM stations ORDER BY username, station_id"
            results = self.execute_query(query, fetch=True)
        
        if not results:
            return []
        
        # Convert to dictionary format
        stations = []
        for row in results:
            stations.append({
                'station_id': row[0],
                'username': row[1],
                'is_selected': row[2]
            })
        return stations

    def delete_station(self, station_id: str, username: str) -> bool:
        query = "DELETE FROM stations WHERE station_id = %s AND username = %s"
        result = self.execute_query(query, (station_id, username))
        return result is True


    # ===========================================================
    # Application Settings
    # ===========================================================
    def set_setting(self, key: str, value: str) -> bool:
        query = """
            INSERT INTO app_settings (key, value, updated_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (key) DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = CURRENT_TIMESTAMP
        """
        result = self.execute_query(query, (key, value))
        return result is True

    def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        query = "SELECT value FROM app_settings WHERE key = %s"
        results = self.execute_query(query, (key,), fetch=True)
        if results and len(results) > 0:
            return results[0][0]
        return default
    
    # ===========================================================
    # Connection Test and Close
    # ===========================================================
    def test_connection(self) -> bool:
        """Test if database connection is valid."""
        try:
            if not self.connection:
                self.connect()
            if self.connection is None:
                return False
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1;")
                results = cursor.fetchall()
                return len(results) > 0
        except Exception as e:
            logging.error(f"Database connection test failed: {e}")
            return False

    def close(self):
        """Close database connection safely."""
        if self.connection:
            try:
                self.connection.close()
                logging.info("Database connection closed.")
            except Exception as e:
                logging.error(f"Error closing database: {e}")
            finally:
                self.connection = None

    # ===========================================================
    # Selection Update Helpers
    # ===========================================================
    def update_server_selection(self, selected_username: str, is_selected: bool = True) -> bool:
        """Update server selection status."""
        if is_selected:
            query_set = "UPDATE servers SET is_selected = TRUE WHERE username = %s"
            result = self.execute_query(query_set, (selected_username,))
        else:
            query_set = "UPDATE servers SET is_selected = FALSE WHERE username = %s"
            result = self.execute_query(query_set, (selected_username,))
        return result is True

    def update_station_selection(self, station_id: str, username: str, is_selected: bool = True) -> bool:
        """Update selected state for a station."""
        query = """
            UPDATE stations
            SET is_selected = %s
            WHERE station_id = %s AND username = %s
        """
        result = self.execute_query(query, (is_selected, station_id, username))
        return result is True

    def execute_query_safe(self, query: str, params: Optional[tuple] = None, fetch: bool = False, max_retries: int = 3):
        """Execute query with automatic retry on connection loss"""
        for attempt in range(max_retries):
            try:
                if not self._ensure_connection():
                    if attempt < max_retries - 1:
                        time.sleep(0.5)
                        continue
                    else:
                        logging.error("Database connection failed after retries")
                        return None

                with self.connection.cursor() as cursor:
                    cursor.execute(query, params)
                    if fetch:
                        results = cursor.fetchall()
                        self.connection.commit()
                        return results
                    self.connection.commit()
                    return True
                    
            except psycopg2.OperationalError as e:
                logging.warning(f"Database connection lost (attempt {attempt + 1}/{max_retries}): {e}")
                self.connection = None
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                else:
                    logging.error(f"Database query failed after {max_retries} retries")
                    return None
                    
            except Exception as e:
                logging.error(f"Database query error: {e}")
                try:
                    if self.connection:
                        self.connection.rollback()
                except:
                    pass
                return None
        
        return None


# ===========================================================
# WINDOWS-COMPATIBLE THREAD-SAFE LOGGING
# ===========================================================

_log_lock = threading.Lock()

def append_download_log(username, station_id, filename, local_path, status, message):
    """Thread-safe download log with Windows file locking support"""
    log_file = "download_log.json"
    max_retries = 5
    retry_delay = 0.1  # 100ms between retries
    
    # Use global lock to prevent concurrent writes from threads
    with _log_lock:
        for attempt in range(max_retries):
            try:
                # Sanitize all inputs - remove problematic characters
                safe_username = str(username).replace('"', "'").replace('\n', ' ').replace('\r', '').strip()
                safe_station = str(station_id).replace('"', "'").replace('\n', ' ').replace('\r', '').strip()
                safe_filename = str(filename).replace('"', "'").replace('\n', ' ').replace('\r', '').strip()
                safe_path = str(local_path).replace('"', "'").replace('\n', ' ').replace('\r', '').strip()
                safe_status = str(status).replace('"', "'").replace('\n', ' ').replace('\r', '').strip()
                safe_message = str(message).replace('"', "'").replace('\n', ' ').replace('\r', '').strip()
                
                log_entry = {
                    "username": safe_username,
                    "station_id": safe_station,
                    "filename": safe_filename,
                    "local_path": safe_path,
                    "status": safe_status,
                    "message": safe_message,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                data = []
                
                # Try to read existing log with retry
                if os.path.exists(log_file):
                    try:
                        with open(log_file, "r", encoding="utf-8") as f:
                            content = f.read().strip()
                            if content:
                                data = json.loads(content)
                            if not isinstance(data, list):
                                data = []
                    except PermissionError:
                        # File is locked, wait and retry
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay * (attempt + 1))
                            continue
                        else:
                            print(f"[WARN] Could not read log after {max_retries} attempts")
                            data = []
                    except json.JSONDecodeError:
                        # Corrupted file - backup and start fresh
                        backup_file = f"download_log_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json.bak"
                        try:
                            os.rename(log_file, backup_file)
                            print(f"[WARN] Corrupted log backed up as: {backup_file}")
                        except:
                            pass
                        data = []
                    except Exception as e:
                        print(f"[ERROR] Error reading log: {e}")
                        data = []
                
                # Append new entry
                data.append(log_entry)
                
                # Keep only last 50,000 entries
                if len(data) > 50000:
                    data = data[-50000:]
                
                # Write safely using temp file with Windows-compatible approach
                temp_file = log_file + f".tmp.{os.getpid()}.{threading.get_ident()}"
                
                try:
                    # Write to temp file
                    with open(temp_file, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    
                    # Try atomic replace with Windows compatibility
                    try:
                        # On Windows, delete first if file exists
                        if os.path.exists(log_file):
                            try:
                                os.remove(log_file)
                            except PermissionError:
                                # File is locked, wait and retry
                                if attempt < max_retries - 1:
                                    time.sleep(retry_delay * (attempt + 1))
                                    if os.path.exists(temp_file):
                                        try:
                                            os.remove(temp_file)
                                        except:
                                            pass
                                    continue
                                else:
                                    raise
                        
                        # Rename temp to actual file
                        os.rename(temp_file, log_file)
                        
                        # Success! Break out of retry loop
                        break
                        
                    except Exception as rename_err:
                        print(f"[ERROR] Failed to rename (attempt {attempt + 1}): {rename_err}")
                        if os.path.exists(temp_file):
                            try:
                                os.remove(temp_file)
                            except:
                                pass
                        
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay * (attempt + 1))
                            continue
                        else:
                            raise
                    
                except Exception as write_err:
                    print(f"[ERROR] Failed to write log (attempt {attempt + 1}): {write_err}")
                    if os.path.exists(temp_file):
                        try:
                            os.remove(temp_file)
                        except:
                            pass
                    
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (attempt + 1))
                        continue
                    else:
                        print(f"[ERROR] Download log failed after {max_retries} attempts")
                        break
                
            except Exception as e:
                print(f"[ERROR] Download log failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    import traceback
                    traceback.print_exc()
                    break