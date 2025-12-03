from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date
import os
import logging
import ftplib
from typing import Tuple, List, Optional
import threading
import re
import json
import time
import socket

logger = logging.getLogger(__name__)


# === Helper: Safe mkdir ===
def safe_makedirs(path):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        logger.error(f"Failed to create directory {path}: {e}")


# === Helper: Connect with retries ===
def ftp_connect(host, user, passwd, port=21, retries=3, timeout=30):
    """Connect to FTP with timeout protection and binary mode"""
    last_exc = None
    for attempt in range(retries):
        ftp = None
        try:
            ftp = ftplib.FTP()
            ftp.connect(host, port, timeout=timeout)
            ftp.login(user, passwd)
            
            # ✅ CRITICAL: Set binary mode for file transfers
            ftp.voidcmd('TYPE I')
            
            # Test connection
            ftp.pwd()
            logger.debug(f"FTP connected successfully to {host}:{port}")
            return ftp
        except socket.timeout as e:
            last_exc = e
            logger.warning(f"FTP connection timeout (attempt {attempt+1}/{retries})")
            if ftp:
                try:
                    ftp.quit()
                except:
                    pass
            time.sleep(2 * (attempt + 1))  # Exponential backoff
        except Exception as e:
            last_exc = e
            logger.warning(f"FTP connection failed (attempt {attempt+1}/{retries}): {e}")
            if ftp:
                try:
                    ftp.quit()
                except:
                    pass
            time.sleep(1)
    
    raise Exception(f"FTP connection failed after {retries} retries: {last_exc}")



def test_ftp_connection(host: str, user: str, passwd: str, port: int = 21) -> Tuple[bool, str]:
    """Test FTP connection and return (status, message)"""
    try:
        ftp = ftp_connect(host, user, passwd, port=port, retries=1)
        if ftp:
            ftp.quit()
            return True, f"Successfully connected to {host}:{port} as {user}"
        else:
            return False, f"Connection failed: ftp_connect() returned None"
    except Exception as e:
        return False, f"Connection error: {str(e)}"


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


# === Helper: Parse filename to extract station_id and datetime ===
def parse_filename(filename: str) -> Optional[Tuple[str, datetime]]:
    """
    Parse filename to extract station_id and datetime.
    
    Formats:
    1. QSRA0004251118104500.txt -> QSRA0004, 2025-11-18 10:45:00
    2. TSET0013RF251108170000_20251108170535.txt -> TSET0013RF, 2025-11-08 17:00:00
    3. TBST0003251129000000.txt -> TBST0003, 2025-11-29 00:00:00 (15-min intervals: 000000, 001500, 003000, 004500)
    """
    try:
        # Remove extension
        name_without_ext = filename.replace('.txt', '').replace('.TXT', '')
        
        # Pattern 1: StationID + YYMMDDHHMMSS (e.g., QSRA0004251118104500)
        # Station ID can be variable length, followed by 12 digits
        match1 = re.match(r'^([A-Z]+\d+(?:RF)?)(\d{12})', name_without_ext)
        if match1:
            station_id = match1.group(1)
            date_str = match1.group(2)
            # Parse: YYMMDDHHMMSS
            # Note: Handles 15-minute intervals (00, 15, 30, 45 minutes)
            try:
                dt = datetime.strptime(date_str, '%y%m%d%H%M%S')
                return station_id, dt
            except ValueError:
                # If parsing fails, might be invalid time format
                logger.debug(f"Invalid datetime in filename {filename}: {date_str}")
                return None
        
        # Pattern 2: StationID + YYMMDDHHMMSS_timestamp (e.g., TSET0013RF251108170000_20251108170535)
        match2 = re.match(r'^([A-Z]+\d+[A-Z]*)(\d{12})_\d+', name_without_ext)
        if match2:
            station_id = match2.group(1)
            date_str = match2.group(2)
            # Parse: YYMMDDHHMMSS
            try:
                dt = datetime.strptime(date_str, '%y%m%d%H%M%S')
                return station_id, dt
            except ValueError:
                logger.debug(f"Invalid datetime in filename {filename}: {date_str}")
                return None
        
        return None
    except Exception as e:
        logger.debug(f"Failed to parse filename {filename}: {e}")
        return None


# === Helper: Generate possible remote paths ===
def generate_possible_paths(base_dir: str, station_id: str, date_obj: date) -> List[str]:
    """
    Generate all possible remote paths based on your actual server structure.
    """
    yyyy = date_obj.strftime("%Y")
    mm = date_obj.strftime("%m")
    dd = date_obj.strftime("%d")
    ddmmyyyy = date_obj.strftime("%d%m%Y")
    
    paths = [
        # Format: /ARCHIVE/2025/11/18/
        f"{base_dir}/ARCHIVE/{yyyy}/{mm}/{dd}",
        f"{base_dir}/Archive/{yyyy}/{mm}/{dd}",
        f"{base_dir}/archive/{yyyy}/{mm}/{dd}",
        
        # Format: /rtutrg/received/2025/11/18112025/
        f"{base_dir}/received/{yyyy}/{mm}/{ddmmyyyy}",
        f"{base_dir}/{station_id}/received/{yyyy}/{mm}/{ddmmyyyy}",
        
        # Format: /archived/2025/11/18112025/
        f"{base_dir}/archived/{yyyy}/{mm}/{ddmmyyyy}",
        f"{base_dir}/Archived/{yyyy}/{mm}/{ddmmyyyy}",
        
        # Root directory (files directly in base_dir)
        f"{base_dir}",
        
        # Common variations
        f"{base_dir}/{yyyy}/{mm}/{dd}",
        f"{base_dir}/{yyyy}/{mm}/{ddmmyyyy}",
        f"{base_dir}/data/{yyyy}/{mm}/{dd}",
        f"{base_dir}/DATA/{yyyy}/{mm}/{dd}",
        
        # Station-specific folders
        f"{base_dir}/{station_id}",
        f"{base_dir}/{station_id}/{yyyy}/{mm}/{dd}",
        f"{base_dir}/{station_id}/{yyyy}/{mm}/{ddmmyyyy}",
    ]
    
    # Remove duplicates while preserving order
    return list(dict.fromkeys(paths))


# === Helper: Download one file - SIMPLIFIED (no subfolders) ===
def download_one_file(host, user, passwd, port, remote_path, filename,
                      local_base_dir, station_id, retries, pause_event, cancel_event, db, 
                      progress_callback=None):
    """
    Download a single file from FTP server.
    All files stored directly in: local_base_dir/filename
    progress_callback: function(bytes_downloaded, total_bytes, filename) for real-time updates
    """
    local_path = None
    try:
        if cancel_event and cancel_event.is_set():
            return False, None

        # Store all files directly in the station folder
        local_dir = local_base_dir
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, filename)

        # Check if file already exists
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            logger.info(f"⭐ Skipping (already exists): {filename}")
            return True, local_path
        
        # Delete 0-byte corrupted files before attempting download
        if os.path.exists(local_path):
            file_size = os.path.getsize(local_path)
            if file_size == 0:
                logger.warning(f"🗑️ Deleting corrupted 0-byte file: {filename}")
                try:
                    os.remove(local_path)
                except Exception as del_err:
                    logger.error(f"Failed to delete corrupted file: {del_err}")

        # Connect to FTP with exponential backoff
        ftp = None
        for attempt in range(retries):
            try:
                ftp = ftplib.FTP()
                ftp.connect(host, port, timeout=30)
                ftp.login(user, passwd)
                
                # ✅ CRITICAL: Set binary transfer mode
                ftp.voidcmd('TYPE I')
                
                ftp.cwd(remote_path)
                
                # ✅ Verify connection is working
                ftp.pwd()
                break
            except Exception as e:
                error_msg = str(e).lower()
                
                # Check for server connection limit errors
                if 'maximum number' in error_msg or 'too many' in error_msg or 'bind' in error_msg:
                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                    logger.warning(f"⏳ Server busy, waiting {wait_time}s before retry (attempt {attempt+1}/{retries})")
                    time.sleep(wait_time)
                else:
                    logger.warning(f"FTP connect attempt {attempt+1} failed: {e}")
                
                if ftp:
                    try:
                        ftp.quit()
                    except:
                        pass
                    ftp = None
                    
                if attempt == retries - 1:
                    # Log the specific error type
                    if 'maximum number' in error_msg or 'bind' in error_msg:
                        raise Exception(f"Server connection limit exceeded (too many concurrent connections)")
                    raise Exception(f"Failed to connect after {retries} attempts: {e}")
                time.sleep(1)

        if ftp is None:
            raise Exception("Failed to establish FTP connection")

        # ✅ Get file size BEFORE download to detect empty files on server
        try:
            file_size = ftp.size(filename)
            if file_size == 0:
                logger.warning(f"⚠️ File on server is 0 bytes (empty): {filename}")
                ftp.quit()
                # ✅ FIX: Return as "skipped" not "failed"
                # We'll return a special marker to distinguish from real failures
                return "skipped", local_path
        except Exception as size_err:
            # SIZE command not supported or failed - continue anyway
            logger.debug(f"Could not get file size for {filename}: {size_err}")
            file_size = 0

        # Download file
        bytes_written = [0]  # Track actual bytes written
        
        with open(local_path, "wb") as f:
            # ✅ Get file size first for accurate progress
            bytes_downloaded = [0]  # Use list to allow modification in nested function
            
            def callback(data):
                if pause_event:
                    while pause_event.is_set():
                        if cancel_event and cancel_event.is_set():
                            raise Exception("Cancelled during pause")
                        threading.Event().wait(0.1)

                if cancel_event and cancel_event.is_set():
                    raise Exception("Download cancelled")
                
                f.write(data)
                bytes_written[0] += len(data)
                bytes_downloaded[0] += len(data)
                
                # ✅ REAL-TIME PROGRESS: Call progress callback immediately
                if progress_callback and file_size > 0:
                    progress_callback(bytes_downloaded[0], file_size, filename)

            # ✅ Use RETR with binary mode
            ftp.retrbinary(f"RETR {filename}", callback, blocksize=8192)

        ftp.quit()
        
        # ✅ Verify file was downloaded successfully (not 0 bytes)
        if os.path.exists(local_path):
            actual_size = os.path.getsize(local_path)
            
            # Check if file is empty
            if actual_size == 0:
                logger.error(f"❌ Downloaded file is 0 bytes (corrupted): {filename}")
                logger.error(f"   Expected size: {file_size if file_size > 0 else 'unknown'}")
                logger.error(f"   Bytes written during download: {bytes_written[0]}")
                logger.error(f"   This may indicate:")
                logger.error(f"     - File is empty on FTP server")
                logger.error(f"     - FTP transfer mode issue (ASCII vs BINARY)")
                logger.error(f"     - Network connection interrupted")
                logger.error(f"     - Firewall blocking data transfer")
                
                try:
                    os.remove(local_path)
                    logger.debug(f"   Deleted corrupted 0-byte file")
                except:
                    pass
                return False, local_path
            
            # Check if size matches (if we knew the size beforehand)
            if file_size > 0 and actual_size != file_size:
                logger.warning(f"⚠️ File size mismatch for {filename}")
                logger.warning(f"   Expected: {file_size} bytes, Got: {actual_size} bytes")
                # Don't fail - file might still be valid
            
            # File seems OK
            logger.debug(f"✅ Downloaded: {filename} ({actual_size} bytes)")
        
        logger.info(f"✅ Downloaded: {filename}")
        return True, local_path

    except Exception as e:
        error_str = str(e).lower()
        
        # Better error messages
        if 'maximum number' in error_str or 'bind' in error_str:
            logger.error(f"🚫 Server connection limit: {filename} (reduce concurrent downloads)")
        elif 'timeout' in error_str:
            logger.error(f"⏱️ Timeout: {filename}")
        else:
            logger.error(f"❌ Error downloading {filename}: {e}")
        
        # Clean up failed download
        if local_path and os.path.exists(local_path):
            try:
                # Only delete if file is 0 bytes (incomplete)
                if os.path.getsize(local_path) == 0:
                    os.remove(local_path)
            except:
                pass
        return False, local_path


# === Main download function - SIMPLIFIED ===
def download_files_by_prefix(host, username=None, password=None, remote_path='/', station_id=None,
                             start_dt=None, end_dt=None, local_base='.', port=21,
                             retries=3, pause_event=None, cancel_event=None, progress_callback=None):
    """
    Download files from FTP for a given station_id and date/time range.
    All files stored directly in: local_base/station_id/date_range/filename
    """
    total_downloaded = []
    total_failed = []

    # Normalize datetime inputs
    def normalize_dt(dt):
        if isinstance(dt, str):
            for fmt in ("%Y-%m-%d", "%d%m%Y", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(dt, fmt)
                except Exception:
                    continue
            raise ValueError(f"Unsupported date string format: {dt}")
        elif isinstance(dt, date) and not isinstance(dt, datetime):
            return datetime.combine(dt, datetime.min.time())
        elif isinstance(dt, datetime):
            return dt
        else:
            raise ValueError("Unsupported date type")

    if start_dt is None or end_dt is None:
        raise ValueError("start_dt and end_dt must be provided")

    start_dt_obj = normalize_dt(start_dt)
    end_dt_obj = normalize_dt(end_dt)

    logger.info(f"📂 Starting download for station {station_id}")
    logger.info(f"📅 Date range: {start_dt_obj} to {end_dt_obj}")
    logger.info(f"🌐 Server: {host}:{port}")
    logger.info(f"📁 Remote path: {remote_path}")

    # Create local directory structure: local_base/station_id/DDMMYYYY_DDMMYYYY/
    start_str = start_dt_obj.strftime("%d%m%Y")
    end_str = end_dt_obj.strftime("%d%m%Y")
    local_station_dir = os.path.join(local_base, station_id, f"{start_str}_{end_str}")
    os.makedirs(local_station_dir, exist_ok=True)
    logger.info(f"💾 Local folder: {local_station_dir}")

    # Collect all files to download
    all_files_to_download = []
    skipped_existing = []
    
    cur_date = start_dt_obj.date()

    while cur_date <= end_dt_obj.date():
        if cancel_event and cancel_event.is_set():
            logger.warning("🛑 Download cancelled during date scan")
            break
        
        possible_paths = generate_possible_paths(remote_path, station_id, cur_date)
        
        logger.info(f"📅 Checking date: {cur_date}")
        logger.debug(f"🔎 Trying {len(possible_paths)} possible paths...")
        
        found = False
        for idx, path in enumerate(possible_paths):
            try:
                ftp = ftp_connect(host, username, password, port=port, retries=1)
                try:
                    ftp.cwd(path)
                    files = ftp.nlst()
                    
                    if files:
                        logger.info(f"✅ Found path: {path} ({len(files)} items)")
                        
                        # Filter files for this station and date/time range
                        for fname in files:
                            if not fname.lower().endswith('.txt'):
                                continue
                            
                            # Parse filename
                            parsed = parse_filename(fname)
                            if parsed:
                                file_station_id, file_dt = parsed
                                
                                # Match station allowing for RF suffix
                                station_base = station_id.upper().replace('RF', '')
                                file_station_base = file_station_id.upper().replace('RF', '')
                                
                                # Check if station matches (with or without RF)
                                if file_station_base == station_base or file_station_id.upper() == station_id.upper():
                                    # Check if datetime is in range
                                    if start_dt_obj <= file_dt <= end_dt_obj:
                                        all_files_to_download.append((path, fname, file_station_id))
                                        logger.debug(f"    ✅ Will download: {fname}")
                                    else:
                                        logger.debug(f"    ✗ Out of time range: {fname}")
                                else:
                                    logger.debug(f"    ✗ Different station ({file_station_id} vs {station_id}): {fname}")
                            else:
                                # If can't parse, check if filename starts with station_id
                                if fname.upper().startswith(station_id.upper()):
                                    all_files_to_download.append((path, fname, station_id))
                                    logger.debug(f"    ✅ Will download: {fname}")
                        
                        found = True
                        break
                finally:
                    try:
                        ftp.quit()
                    except Exception:
                        pass
            except Exception as e:
                logger.debug(f"    ✗ Path failed: {path} - {str(e)[:50]}")
                continue
        
        if not found:
            logger.warning(f"⚠️ No files found for {cur_date}")
        
        cur_date += timedelta(days=1)

    # Check for existing files
    filtered_files_to_download = []
    
    for path, fname, file_station_id in all_files_to_download:
        local_file_path = os.path.join(local_station_dir, fname)
        
        if os.path.exists(local_file_path) and os.path.getsize(local_file_path) > 0:
            skipped_existing.append(fname)
            logger.debug(f"    ⭐ Already exists: {fname}")
        else:
            filtered_files_to_download.append((path, fname, file_station_id))
    
    all_files_to_download = filtered_files_to_download

    if not all_files_to_download:
        if skipped_existing:
            logger.info(f"✅ All {len(skipped_existing)} files already exist locally for station {station_id}")
            logger.info(f"⭐ Skipping this station - no new files to download")
            # ✅ Return empty lists to indicate "nothing new to download"
            # This prevents the GUI from thinking files were "downloaded"
            return [], []
        else:
            logger.warning(f"⚠️ No files found for station {station_id}")
            return [], []

    total_files = len(all_files_to_download)
    logger.info(f"📦 Found {total_files} NEW files to download ({len(skipped_existing)} already exist)")
    logger.info(f"⭐ Skipping {len(skipped_existing)} files that already exist locally")

    # Threading configuration - reduce threads to avoid server connection limits
    if total_files > 10000:
        max_threads = 3
        batch_update_interval = 50  # ✅ More frequent updates
    elif total_files > 5000:
        max_threads = 5
        batch_update_interval = 25  # ✅ More frequent updates
    elif total_files > 2000:
        max_threads = 6
        batch_update_interval = 15  # ✅ More frequent updates
    elif total_files > 1000:
        max_threads = 8
        batch_update_interval = 10  # ✅ More frequent updates
    elif total_files > 500:
        max_threads = 10
        batch_update_interval = 5  # ✅ More frequent updates
    elif total_files > 100:
        max_threads = 12
        batch_update_interval = 3  # ✅ More frequent updates
    else:
        max_threads = 8
        batch_update_interval = 1  # ✅ Update for every file
    
    logger.info(f"🚀 Starting {max_threads} download threads...")
    logger.info(f"💡 Using conservative threading to avoid server connection limits")
    
    # Progress tracking
    progress_lock = threading.Lock()
    downloaded_count = [0]
    failed_count = [0]
    skipped_count = [0]  # ✅ Track empty files separately
    last_update_count = [0]
    
    def update_progress_batch():
        with progress_lock:
            current_total = downloaded_count[0] + failed_count[0] + skipped_count[0]
            # Update when we've processed enough files OR completed all
            if (current_total - last_update_count[0] >= batch_update_interval) or (current_total == total_files):
                if progress_callback:
                    # Pass: (files_processed, total_files_in_station, status)
                    progress_callback(current_total, total_files, "batch")
                last_update_count[0] = current_total
                if current_total % 100 == 0 or current_total == total_files:
                    logger.info(
                        f"📈 Progress: {current_total}/{total_files} files "
                        f"({downloaded_count[0]} ✅, {failed_count[0]} ❌, {skipped_count[0]} ⭐)"
                    )
    
    # Process in chunks
    chunk_size = 2000 if total_files > 5000 else total_files
    
    for chunk_start in range(0, total_files, chunk_size):
        chunk_end = min(chunk_start + chunk_size, total_files)
        chunk = all_files_to_download[chunk_start:chunk_end]
        
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = {}
            for remote_path_found, filename, file_station_id in chunk:
                # ✅ Create progress callback that updates immediately
                def make_progress_callback(fname):
                    """Factory to create progress callback with correct filename"""
                    def callback(received, total, fn):
                        with progress_lock:
                            if progress_callback:
                                progress_callback(
                                    downloaded_count[0] + failed_count[0], 
                                    total_files, 
                                    fname
                                )
                    return callback
                
                fut = executor.submit(
                    download_one_file, host, username, password, port,
                    remote_path_found, filename, local_station_dir, file_station_id,
                    retries, pause_event, cancel_event, None,
                    make_progress_callback(filename)  # ✅ Pass progress callback
                )
                futures[fut] = (remote_path_found, filename)

            for fut in as_completed(futures):
                remote_path_found, filename = futures[fut]
                try:
                    result, local_path = fut.result()
                    
                    # ✅ UPDATE IMMEDIATELY when file completes
                    with progress_lock:
                        if result == True or result == "skipped":
                            # Success or skipped (empty file)
                            if result == "skipped":
                                skipped_count[0] += 1
                                logger.debug(f"⭐ Skipped (empty on server): {filename}")
                            else:
                                if local_path and os.path.exists(local_path):
                                    total_downloaded.append(local_path)
                                    downloaded_count[0] += 1
                                    logger.debug(f"✅ {filename}")
                                else:
                                    total_downloaded.append(local_path)
                                    downloaded_count[0] += 1
                        else:
                            # Real failure
                            total_failed.append(filename)
                            failed_count[0] += 1
                            logger.debug(f"❌ {filename}")
                        
                        # ✅ IMMEDIATE UPDATE - Don't wait for batch
                        current_total = downloaded_count[0] + failed_count[0] + skipped_count[0]
                        if progress_callback:
                            progress_callback(current_total, total_files, filename)
                    
                    # Also do batch updates for logging
                    update_progress_batch()
                    
                except Exception as e:
                    logger.exception(f"Thread error for {filename}")
                    total_failed.append(filename)
                    with progress_lock:
                        failed_count[0] += 1
                    update_progress_batch()

    logger.info(f"✅ Download complete: {len(total_downloaded)} success, {len(total_failed)} failed")
    if skipped_count[0] > 0:
        logger.info(f"⭐ Skipped {skipped_count[0]} empty files (0 bytes on server)")
    logger.info(f"📊 All files stored directly in station folder")
    return total_downloaded, total_failed


# === Get remote directory listing ===
def get_remote_directory_listing(host: str, username: str, password: str, remote_path: str = "/", port: int = 21):
    """List files in a remote directory"""
    ftp = None
    try:
        ftp = ftplib.FTP()
        ftp.connect(host, port, timeout=15)
        ftp.login(username, password)
        ftp.cwd(remote_path)
        files = ftp.nlst()
        ftp.quit()
        return True, files, f"Listed {len(files)} items from {remote_path}"
    except Exception as e:
        return False, [], f"FTP listing failed: {str(e)}"
    finally:
        if ftp:
            try:
                ftp.quit()
            except:
                pass