import sys
import os
import json
import threading
import csv
import shutil
import logging
from datetime import datetime, date, timedelta
from typing import Optional, cast
from database import DatabaseManager, append_download_log

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ftp_downloader.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# PyQt6 imports
from PyQt6.QtWidgets import (
    QApplication, QWidget, QMainWindow, QLabel, QPushButton, QLineEdit,
    QVBoxLayout, QHBoxLayout, QFormLayout, QTableWidget,
    QTableWidgetItem, QHeaderView, QMessageBox, QFileDialog, QComboBox,
    QDateEdit, QTimeEdit, QCheckBox, QTabWidget, QGroupBox, QScrollArea,
    QSpinBox, QProgressBar, QTextEdit, QDialog, QSizePolicy, QAbstractSpinBox, QGridLayout
)
from PyQt6.QtCore import Qt, QTimer, QThread, QObject, pyqtSignal, QDate, QTime
from PyQt6.QtGui import QIcon, QFont

# Local imports
from ftp_downloader import (
    download_files_by_prefix, test_ftp_connection, 
    get_remote_directory_listing
)



class PasswordLineEdit(QWidget):
    """Custom password field with show/hide toggle"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()
    
    def setup_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        self.line_edit = QLineEdit()
        self.line_edit.setEchoMode(QLineEdit.EchoMode.Password)
        layout.addWidget(self.line_edit)

        button_style = """
        QPushButton {
            background-color: #f2f2f2;
            border: 1px solid #cccccc;
            border-radius: 6px;
            padding: 4px 8px;
            font-size: 10px;
            color:#000000; 
        }
        QPushButton:hover {
            background-color: #e6e6e6;
            border-color: #999999;
        }
        QPushButton:pressed {
            background-color: #d9d9d9;
            border-color: #888888;
        }"""
        
        self.toggle_btn = QPushButton("Show")
        self.toggle_btn.setStyleSheet(button_style)
        self.toggle_btn.setFixedSize(30, 25)
        self.toggle_btn.setCheckable(True)
        self.toggle_btn.clicked.connect(self.toggle_visibility)
        layout.addWidget(self.toggle_btn)
    
    def toggle_visibility(self):
        button_style = """
        QPushButton {
            background-color: #f2f2f2;
            border: 1px solid #cccccc;
            border-radius: 6px;
            padding: 4px 8px;
            font-size: 10px;
            color:#000000; 
        }
        QPushButton:hover {
            background-color: #e6e6e6;
            border-color: #999999;
        }
        QPushButton:pressed {
            background-color: #d9d9d9;
            border-color: #888888;
        }"""
        
        if self.toggle_btn.isChecked():
            self.line_edit.setEchoMode(QLineEdit.EchoMode.Normal)
            self.toggle_btn.setText("Hide")
        else:
            self.line_edit.setEchoMode(QLineEdit.EchoMode.Password)
            self.toggle_btn.setText("Show")
        
        self.toggle_btn.setStyleSheet(button_style)
    
    def text(self):
        return self.line_edit.text()
    
    def setText(self, text):
        self.line_edit.setText(text)
    
    def clear(self):
        self.line_edit.clear()


class CheckboxListWidget(QWidget):
    """Custom widget for checkbox list with select all functionality"""
    
    def __init__(self, title="Items"):
        super().__init__()
        self.title = title
        self.items = []
        self.setup_ui()
    
    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        layout.setSpacing(5)

        # Title
        if self.title:
            title_label = QLabel(self.title)
            title_label.setFont(QFont("Arial", 10, QFont.Weight.Bold))
            layout.addWidget(title_label)

        # Select All checkbox
        self.select_all_cb = QCheckBox("Select All")
        self.select_all_cb.stateChanged.connect(self.toggle_select_all)
        layout.addWidget(self.select_all_cb)

        # Scrollable area for items
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        scroll_widget = QWidget()
        self.items_layout = QVBoxLayout(scroll_widget)
        self.items_layout.addStretch()
        self.scroll_area.setWidget(scroll_widget)

        layout.addWidget(self.scroll_area, stretch=1)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
    
    def add_item(self, text, data=None, checked=False):
        """Add an item to the list (inserts before the bottom stretch)."""
        cb = QCheckBox(text)
        cb.setChecked(checked)
        cb.setProperty("data", data)
        cb.stateChanged.connect(self.update_select_all_state)
        cb.setEnabled(True)

        self.items.append(cb)
        insert_index = max(0, self.items_layout.count() - 1)
        self.items_layout.insertWidget(insert_index, cb)
        self.update_select_all_state()
    
    def clear_items(self):
        """Clear all items"""
        for item in self.items:
            item.setParent(None)
            item.deleteLater()
        self.items = []
        self.select_all_cb.blockSignals(True)
        self.select_all_cb.setChecked(False)
        self.select_all_cb.blockSignals(False)
    
    def toggle_select_all(self, state):
        """Select or deselect all items when Select All checkbox is clicked."""
        if not self.items:
            return

        checked = (state == Qt.CheckState.Checked.value)
        self.select_all_cb.blockSignals(True)
        
        for item in self.items:
            item.blockSignals(True)
            item.setChecked(checked)
            item.blockSignals(False)

        self.select_all_cb.blockSignals(False)

    def update_select_all_state(self):
        """Update select all checkbox state based on individual items."""
        if not self.items:
            self.select_all_cb.blockSignals(True)
            self.select_all_cb.setCheckState(Qt.CheckState.Unchecked)
            self.select_all_cb.blockSignals(False)
            return

        checked_count = sum(1 for item in self.items if item.isChecked())

        self.select_all_cb.blockSignals(True)
        if checked_count == 0:
            self.select_all_cb.setCheckState(Qt.CheckState.Unchecked)
        elif checked_count == len(self.items):
            self.select_all_cb.setCheckState(Qt.CheckState.Checked)
        else:
            self.select_all_cb.setCheckState(Qt.CheckState.PartiallyChecked)
        self.select_all_cb.blockSignals(False)

    def get_checked_items(self):
        """Get list of checked items with their data"""
        return [(item.text(), item.property("data")) for item in self.items if item.isChecked()]
    
    def get_checked_data(self):
        """Get list of data from checked items"""
        return [item.property("data") for item in self.items if item.isChecked() and item.property("data") is not None]


class DownloadWorker(QObject):
    """Worker thread for FTP downloads"""
    progress_updated = pyqtSignal(str, str, int, int, int, str)  # server_info, status, total, downloaded, failed, current_file
    finished = pyqtSignal(str, int, int)
    log_message = pyqtSignal(str)

    def __init__(self, server_config, stations, params, db_manager: DatabaseManager):
        super().__init__()
        self.server_config = server_config
        self.stations = stations
        self.params = params
        self.db_manager = db_manager
        self.pause_event = threading.Event()
        self.cancel_event = threading.Event()
        self.is_running = False
        self._thread = None

    def set_thread(self, thread: QThread):
        self._thread = thread

    def run(self):
        """Run the download process with accurate progress tracking"""
        self.is_running = True
        server_info = self.server_config['username']
        self.log_message.emit(f"Starting download for server: {server_info}")

        try:
            cumulative_downloaded = 0
            cumulative_failed = 0
            cumulative_total = 0
            cumulative_skipped = 0  # ✅ NEW: Track skipped files
            
            self.log_message.emit("Starting download...")
            self.progress_updated.emit(server_info, "Initializing...", 0, 0, 0, "")

            for station_index, station in enumerate(self.stations, 1):
                if self.cancel_event.is_set():
                    self.log_message.emit("Download cancelled by user")
                    break

                station_status = f"Processing station {station_index}/{len(self.stations)}: {station}"
                self.progress_updated.emit(
                    server_info, 
                    station_status, 
                    cumulative_total if cumulative_total > 0 else cumulative_downloaded + cumulative_failed,
                    cumulative_downloaded, 
                    cumulative_failed, 
                    station
                )
                self.log_message.emit(f"📂 Processing station: {station}")

                # ✅ Create progress callback for real-time updates
                def station_progress_callback(processed, total, current_file):
                    """Real-time progress callback during file download"""
                    self.progress_updated.emit(
                        server_info,
                        f"Station {station_index}/{len(self.stations)}: {station}",
                        cumulative_total if cumulative_total > 0 else processed,
                        cumulative_downloaded + processed,
                        cumulative_failed,
                        current_file
                    )

                try:
                    downloaded, failed = download_files_by_prefix(
                        host=self.server_config['host'],
                        username=self.server_config['username'],
                        password=self.server_config['password'],
                        remote_path=self.server_config.get('remote_path', '/'),
                        station_id=station,
                        start_dt=self.params['start_dt'],
                        end_dt=self.params['end_dt'],
                        local_base=self.params['local_folder'],
                        port=self.server_config['port'],
                        retries=3,
                        pause_event=self.pause_event,
                        cancel_event=self.cancel_event,
                        progress_callback=station_progress_callback  # ✅ Pass callback
                    )
                    
                    # ✅ FIX: Check if this station had NO NEW files to download
                    if len(downloaded) > 0 and len(failed) == 0:
                        # Check if these are "already existed" files (returned as successful but not actually downloaded)
                        # The download function returns existing files in the downloaded list
                        
                        # Check if any actual downloads happened by checking if files are new
                        all_files_existed = all(os.path.exists(f) for f in downloaded if f)
                        
                        if all_files_existed:
                            # All files already existed - count as skipped, not downloaded
                            skipped_count = len(downloaded)
                            cumulative_skipped += skipped_count
                            
                            self.log_message.emit(f"⭐ Station {station}: {skipped_count} files already exist (skipped)")
                            
                            # Don't add to cumulative_total since these aren't "new" files to process
                            continue
                    
                except Exception as e:
                    error_msg = f"Error downloading from station {station}: {str(e)}"
                    self.log_message.emit(error_msg)
                    print(f"[ERROR] {error_msg}")
                    downloaded, failed = [], [f"Station {station}: {str(e)}"]

                # Update cumulative total after processing this station
                station_files = len(downloaded) + len(failed)
                cumulative_total += station_files
                
                self.log_message.emit(f"📊 Station {station}: {len(downloaded)} success, {len(failed)} failed")

                # Process downloaded files
                for file_path in downloaded:
                    try:
                        if self.cancel_event.is_set():
                            break
                            
                        filename = os.path.basename(file_path)
                        safe_username = self.server_config.get('username') or "system"
                        
                        cumulative_downloaded += 1
                        
                        # Update progress with accurate totals
                        self.progress_updated.emit(
                            server_info, 
                            station_status,
                            cumulative_total,
                            cumulative_downloaded,
                            cumulative_failed,
                            filename
                        )
                        
                        try:
                            from database import append_download_log
                            append_download_log(
                                safe_username, station, filename, file_path,
                                'success', 'Downloaded successfully'
                            )
                        except Exception as log_err:
                            print(f"[WARN] Log write failed for {filename}: {log_err}")
                        
                        self.log_message.emit(f"✅ {filename}")
                        
                    except Exception as file_err:
                        print(f"[ERROR] Error processing downloaded file: {file_err}")
                        continue

                # Process failed files
                for failed_file in failed:
                    try:
                        if self.cancel_event.is_set():
                            break
                            
                        safe_username = self.server_config.get('username') or "system"
                        
                        cumulative_failed += 1
                        
                        # Update progress with accurate totals
                        self.progress_updated.emit(
                            server_info,
                            station_status,
                            cumulative_total,
                            cumulative_downloaded,
                            cumulative_failed,
                            failed_file
                        )
                        
                        try:
                            from database import append_download_log
                            append_download_log(
                                safe_username, station, failed_file, '',
                                'failed', 'Download failed'
                            )
                        except Exception as log_err:
                            print(f"[WARN] Log write failed for {failed_file}: {log_err}")
                        
                        self.log_message.emit(f"✗ {failed_file}")
                        
                    except Exception as file_err:
                        print(f"[ERROR] Error processing failed file: {file_err}")
                        continue

            # ✅ FIX: Better final status message
            if cumulative_total == 0 and cumulative_skipped > 0:
                # All files already existed - nothing new downloaded
                final_status = f"✅ Download completed! All {cumulative_skipped} files already exist locally"
                self.log_message.emit(f"⭐ All {cumulative_skipped} files already downloaded - no new files")
            elif not self.cancel_event.is_set():
                if cumulative_failed > 0:
                    final_status = f"⚠️ Download completed with {cumulative_failed} failures"
                else:
                    final_status = "✅ Download completed!"
            else:
                final_status = "⏹️ Download cancelled"
            
            self.progress_updated.emit(
                server_info,
                final_status,
                cumulative_total,
                cumulative_downloaded,
                cumulative_failed,
                ""
            )
            
            self.finished.emit(server_info, cumulative_downloaded, cumulative_failed)
            
            # ✅ Better logging
            if cumulative_total == 0 and cumulative_skipped > 0:
                self.log_message.emit(
                    f"✅ No new files - {cumulative_skipped} files already exist locally"
                )
            else:
                self.log_message.emit(
                    f"Download completed: {cumulative_downloaded} downloaded, {cumulative_failed} failed out of {cumulative_total} total files"
                )

        except Exception as e:
            error_msg = f"Critical error in download worker: {str(e)}"
            print(f"[CRITICAL] {error_msg}")
            import traceback
            traceback.print_exc()
            
            self.progress_updated.emit(server_info, f"Error: {str(e)}", 0, 0, 0, "")
            self.log_message.emit(error_msg)
            self.finished.emit(server_info, 0, 0)
        finally:
            self.is_running = False
            self.log_message.emit("Worker thread finished")
   
    def pause(self):
        self.pause_event.set()
        self.log_message.emit("Download paused")

    def resume(self):
        self.pause_event.clear()
        self.log_message.emit("Download resumed")

    def cancel(self):
        self.cancel_event.set()
        self.log_message.emit("Download cancelled")

    def stop(self):
        """Safely stop the worker and its thread."""
        self.cancel_event.set()

class RetryDownloadWorker(QObject):
    """Worker specifically for retrying failed downloads"""
    progress_updated = pyqtSignal(str, str, int, int, int, str)
    finished = pyqtSignal(str, int, int)
    log_message = pyqtSignal(str)

    def __init__(self, server_config, stations, params, db_manager, retry_files_dict):
        super().__init__()
        self.server_config = server_config
        self.stations = stations
        self.params = params
        self.db_manager = db_manager
        self.retry_files_dict = retry_files_dict  # {station_id: [filenames]}
        self.pause_event = threading.Event()
        self.cancel_event = threading.Event()
        self.is_running = False
        self._thread = None
        self.total_files = sum(len(files) for files in retry_files_dict.values())
        self.downloaded_count = 0
        self.failed_count = 0

    def set_thread(self, thread: QThread):
        self._thread = thread

    def run(self):
        """Run the download process with comprehensive error handling"""
        self.is_running = True
        server_info = self.server_config['username']
        self.log_message.emit(f"Starting download for server: {server_info}")

        try:
            total_downloaded = 0
            total_failed = 0
            
            self.log_message.emit("Starting download...")
            self.progress_updated.emit(server_info, "Scanning for files...", 0, 0, 0, "")

            for station_index, station in enumerate(self.stations, 1):
                if self.cancel_event.is_set():
                    self.log_message.emit("Download cancelled by user")
                    break

                self.progress_updated.emit(
                    server_info, 
                    f"Processing station {station_index}/{len(self.stations)}: {station}", 
                    self.total_files if self.total_files > 0 else total_downloaded + total_failed,
                    total_downloaded, 
                    total_failed, 
                    station
                )
                self.log_message.emit(f"📂 Processing station: {station}")

                def progress_callback(received, total, filename):
                    pass

                try:
                    downloaded, failed = download_files_by_prefix(
                        host=self.server_config['host'],
                        username=self.server_config['username'],
                        password=self.server_config['password'],
                        remote_path=self.server_config.get('remote_path', '/'),
                        station_id=station,
                        start_dt=self.params['start_dt'],
                        end_dt=self.params['end_dt'],
                        local_base=self.params['local_folder'],
                        port=self.server_config['port'],
                        retries=3,
                        pause_event=self.pause_event,
                        cancel_event=self.cancel_event,
                        progress_callback=progress_callback
                    )
                    
                    # ✅ FIX: Check if station was skipped (all files already exist)
                    if len(downloaded) > 0 and len(failed) == 0:
                        # Check if these are "fake" downloads (files that already existed)
                        # If all files already existed, the function returns them as "downloaded"
                        station_files = len(downloaded)
                        self.total_files += station_files
                        total_downloaded += station_files
                        
                        self.log_message.emit(f"✅ Station {station}: {station_files} files (already existed locally)")
                        
                        # Update progress to show we're done with this station
                        self.progress_updated.emit(
                            server_info,
                            f"Station {station_index}/{len(self.stations)}: Complete",
                            self.total_files,
                            total_downloaded,
                            total_failed,
                            ""
                        )
                        
                        # ✅ SKIP the file-by-file processing - files already exist!
                        continue
                        
                    elif len(downloaded) == 0 and len(failed) == 0:
                        # No files found at all for this station
                        self.log_message.emit(f"⚠️  Station {station}: No files found")
                        continue
                    
                    # ✅ Update total files count progressively (only for NEW downloads)
                    station_files = len(downloaded) + len(failed)
                    self.total_files += station_files
                    
                except Exception as e:
                    error_msg = f"Error downloading from station {station}: {str(e)}"
                    self.log_message.emit(error_msg)
                    print(f"[ERROR] {error_msg}")
                    downloaded, failed = [], [f"Station {station}: {str(e)}"]

                # Process downloaded files with error handling
                for file_path in downloaded:
                    try:
                        if self.cancel_event.is_set():
                            break
                            
                        filename = os.path.basename(file_path)
                        safe_username = self.server_config.get('username') or "system"
                        
                        total_downloaded += 1
                        
                        self.progress_updated.emit(
                            server_info, 
                            f"Station {station_index}/{len(self.stations)}: {station}",
                            self.total_files,
                            total_downloaded,
                            total_failed,
                            filename
                        )
                        
                        try:
                            append_download_log(
                                safe_username, station, filename, file_path,
                                'success', 'Downloaded successfully'
                            )
                        except Exception as log_err:
                            print(f"[WARN] Log write failed for {filename}: {log_err}")
                        
                        self.log_message.emit(f"✓ {filename}")
                        
                    except Exception as file_err:
                        print(f"[ERROR] Error processing downloaded file: {file_err}")
                        continue

                # Process failed files with error handling
                for failed_file in failed:
                    try:
                        if self.cancel_event.is_set():
                            break
                            
                        safe_username = self.server_config.get('username') or "system"
                        
                        total_failed += 1
                        
                        self.progress_updated.emit(
                            server_info,
                            f"Station {station_index}/{len(self.stations)}: {station}",
                            self.total_files,
                            total_downloaded,
                            total_failed,
                            failed_file
                        )
                        
                        try:
                            append_download_log(
                                safe_username, station, failed_file, '',
                                'failed', 'Download failed'
                            )
                        except Exception as log_err:
                            print(f"[WARN] Log write failed for {failed_file}: {log_err}")
                        
                        self.log_message.emit(f"✗ {failed_file}")
                        
                    except Exception as file_err:
                        print(f"[ERROR] Error processing failed file: {file_err}")
                        continue

            # Final progress update
            self.progress_updated.emit(
                server_info,
                "✅ Download completed!" if not self.cancel_event.is_set() else "❌ Download cancelled",
                self.total_files,
                total_downloaded,
                total_failed,
                ""
            )
            
            self.finished.emit(server_info, total_downloaded, total_failed)
            self.log_message.emit(
                f"Download completed: {total_downloaded} files processed, {total_failed} failed"
            )

        except Exception as e:
            error_msg = f"Critical error in download worker: {str(e)}"
            print(f"[CRITICAL] {error_msg}")
            import traceback
            traceback.print_exc()
            
            self.progress_updated.emit(server_info, f"Error: {str(e)}", 0, 0, 0, "")
            self.log_message.emit(error_msg)
            self.finished.emit(server_info, 0, 0)
        finally:
            self.is_running = False
            self.log_message.emit("Worker thread finished")

    def pause(self):
        self.pause_event.set()
        self.log_message.emit("Retry paused")

    def resume(self):
        self.pause_event.clear()
        self.log_message.emit("Retry resumed")

    def cancel(self):
        self.cancel_event.set()
        self.log_message.emit("Retry cancelled")

    def stop(self):
        """Safely stop the worker and its thread."""
        self.cancel_event.set()
        self.is_running = False
        self.log_message.emit("Stopping retry worker...")

class ServerWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.local_folder_edit: QLineEdit | None = None
        self.start_date: QDateEdit | None = None
        self.end_date: QDateEdit | None = None
        self.start_time: QTimeEdit | None = None
        self.end_time: QTimeEdit | None = None
        self.selected_stations: CheckboxListWidget | None = None
        self.progress_bar: QProgressBar | None = None
        self.status_label: QLabel | None = None
        self.auto_download_checkbox = QCheckBox("Enable Auto Download")
        self.auto_time_edit = QTimeEdit()
        self.auto_time_edit.setDisplayFormat("HH:mm")


class FTPDownloaderGUI(QMainWindow):
    def __init__(self, db_manager):
        super().__init__()
        self.download_workers = {}
        self.download_threads = {}
        self.selected_username = None
        self.current_username = None
        self.db_manager = db_manager
        self.stations_list = CheckboxListWidget("")
        
        self.init_database()
        
        if self.db_manager:
            self.init_ui()
            self.load_data()
            
            # Auto-refresh history timer
            self.history_timer = QTimer()
            self.history_timer.timeout.connect(self.refresh_history)
            self.history_timer.start(2000)
        else:
            self.show_database_error()

    def safe_cleanup_worker(self, username):
        """Safely clean up worker and thread"""
        try:
            if username in self.download_workers:
                worker = self.download_workers[username]
                try:
                    worker.stop()
                    print(f"[INFO] Stopped worker for {username}")
                except Exception as e:
                    print(f"[WARN] Error stopping worker: {e}")
                
                del self.download_workers[username]
            
            if username in self.download_threads:
                thread = self.download_threads[username]
                try:
                    if thread.isRunning():
                        thread.quit()
                        if not thread.wait(3000):  # Wait 3 seconds
                            print(f"[WARN] Thread {username} did not stop gracefully, terminating...")
                            thread.terminate()
                            thread.wait()
                    print(f"[INFO] Stopped thread for {username}")
                except Exception as e:
                    print(f"[WARN] Error stopping thread: {e}")
                
                del self.download_threads[username]
                
        except Exception as e:
            print(f"[ERROR] Cleanup failed for {username}: {e}")
            
    def get_server_widget(self, server_name: str) -> Optional["ServerWidget"]:
        """Return the ServerWidget instance matching the given server name."""
        for i in range(self.server_tabs.count()):
            if self.server_tabs.tabText(i) == server_name:
                widget = self.server_tabs.widget(i)
                return cast(ServerWidget, widget)
        return None

    def init_database(self):
        """Initialize database with error handling"""
        try:
            self.db_manager = DatabaseManager()
            if not self.db_manager.test_connection():
                self.db_manager = None
        except Exception as e:
            print(f"Database initialization failed: {e}")
            self.db_manager = None
    
    def show_database_error(self):
        """Show database error dialog"""
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Icon.Critical)
        msg.setWindowTitle("Database Error")
        msg.setText("Failed to connect to the database.")
        msg.setInformativeText("Please check your database configuration and try again.")
        msg.setDetailedText("Make sure PostgreSQL is running and the database 'ftp_db' exists.")
        msg.exec()
        sys.exit(1)
    
    def init_ui(self):
        self.setWindowTitle("FTP Downloader v2.0")
        self.setGeometry(100, 100, 850, 700)
        
        # Apply styling
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f0f0f0;
            }
            QTabWidget::pane {
                border: 1px solid #c0c0c0;
                background-color: white;
            }
            QTabBar::tab {
                background-color: #e0e0e0;
                padding: 8px 16px;
                margin-right: 2px;
                border: 1px solid #c0c0c0;
                border-bottom: none;
            }
            QTabBar::tab:selected {
                background-color: #2196F3;
                color: white;
                border-bottom: none;
            }
            QGroupBox {
                font-weight: bold;
                border: 2px solid #c0c0c0;
                border-radius: 5px;
                margin: 10px 0;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px 0 5px;
            }
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
                min-width: 80px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:pressed {
                background-color: #3d8b40;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                color: #666666;
            }
            QLineEdit, QSpinBox, QDateEdit, QTimeEdit, QComboBox {
                padding: 5px;
                border: 1px solid #ccc;
                border-radius: 3px;
            }
        """)
        
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        layout = QVBoxLayout(central_widget)
        
        self.main_tabs = QTabWidget()
        layout.addWidget(self.main_tabs)
        
        self.create_settings_tab()
        self.create_main_tab()
        self.create_history_tab()

    def create_settings_tab(self):
        """Create Settings tab with sub-tabs"""
        settings_widget = QWidget()
        self.main_tabs.addTab(settings_widget, "Settings")
        
        layout = QVBoxLayout(settings_widget)
        
        self.settings_tabs = QTabWidget()
        layout.addWidget(self.settings_tabs)
        
        self.create_server_settings_tab()
        self.create_station_settings_tab()
        self.create_select_server_tab()
    
    def create_server_settings_tab(self):
        """Create Server Settings sub-tab"""
        server_widget = QWidget()
        self.settings_tabs.addTab(server_widget, "Server Settings")
        
        layout = QVBoxLayout(server_widget)
        
        # Server form
        form_group = QGroupBox("Server Configuration")
        form_layout = QGridLayout(form_group)
        form_layout.setColumnStretch(1, 2)  # IP Address field wider
        form_layout.setColumnStretch(3, 1)  # Port field narrower
        form_layout.setHorizontalSpacing(20)
        form_layout.setVerticalSpacing(10)
        
        # Row 0: IP Address (full width)
        ip_label = QLabel("IP Address:")
        self.server_ip_edit = QLineEdit()
        form_layout.addWidget(ip_label, 0, 0)
        form_layout.addWidget(self.server_ip_edit, 0, 1, 1, 3)  # Span 3 columns
        
        # Row 1: Username and Port
        username_label = QLabel("Username:")
        self.server_username_edit = QLineEdit()
        
        port_label = QLabel("Port:")
        self.server_port_edit = QSpinBox()
        self.server_port_edit.setRange(1, 65535)
        self.server_port_edit.setValue(21)
        self.server_port_edit.setFixedWidth(100)
        self.server_port_edit.setAlignment(Qt.AlignmentFlag.AlignLeft)
        self.server_port_edit.setStyleSheet("""
            QSpinBox {
                padding: 6px 8px;
                font-size: 13px;
                border: 1px solid #ccc;
                border-radius: 3px;
                background-color: white;
            }
            QSpinBox:focus {
                border: 1px solid #4CAF50;
            }
            QSpinBox::up-button, QSpinBox::down-button {
                width: 0px;
                border: none;
            }
        """)
        
        form_layout.addWidget(username_label, 1, 0)
        form_layout.addWidget(self.server_username_edit, 1, 1)
        form_layout.addWidget(port_label, 1, 2)
        form_layout.addWidget(self.server_port_edit, 1, 3)
        
        # Row 2: Password (full width)
        password_label = QLabel("Password:")
        self.server_password_edit = PasswordLineEdit()
        form_layout.addWidget(password_label, 2, 0)
        form_layout.addWidget(self.server_password_edit, 2, 1, 1, 3)  # Span 3 columns
        
        # Row 3: Remote Path (full width)
        path_label = QLabel("Remote Path:")
        self.server_path_edit = QLineEdit()
        self.server_path_edit.setPlaceholderText("/path")
        form_layout.addWidget(path_label, 3, 0)
        form_layout.addWidget(self.server_path_edit, 3, 1, 1, 3)  # Span 3 columns
        
        layout.addWidget(form_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        self.add_server_btn = QPushButton("Add Server")
        self.update_server_btn = QPushButton("Update Server")
        self.test_connection_btn = QPushButton("Test Connection")
        self.preview_remote_btn = QPushButton("Preview Directory")
        self.clear_form_btn = QPushButton("Clear Form")
        
        self.add_server_btn.clicked.connect(self.add_server)
        self.update_server_btn.clicked.connect(self.update_server)
        self.test_connection_btn.clicked.connect(self.test_connection)
        self.preview_remote_btn.clicked.connect(self.preview_remote_directory)
        self.clear_form_btn.clicked.connect(self.clear_server_form)
        
        self.update_server_btn.setEnabled(False)
        
        button_layout.addWidget(self.add_server_btn)
        button_layout.addWidget(self.update_server_btn)
        button_layout.addWidget(self.test_connection_btn)
        button_layout.addWidget(self.preview_remote_btn)
        button_layout.addWidget(self.clear_form_btn)
        button_layout.addStretch()
        
        layout.addLayout(button_layout)
        
        # Server list table
        list_group = QGroupBox("Saved Servers")
        list_layout = QVBoxLayout(list_group)

        self.servers_table = QTableWidget()
        self.servers_table.setColumnCount(5)
        self.servers_table.setHorizontalHeaderLabels(
            ["IP Address", "Username", "Remote Path", "Edit", "Delete"]
        )
        self.servers_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.servers_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)

        header = self.servers_table.verticalHeader()
        if header is not None:
            header.setVisible(False)

        header = self.servers_table.horizontalHeader()
        if header:
            header.setStretchLastSection(False)
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
            header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
            header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
            header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)

        self.servers_table.setStyleSheet("""
            QTableWidget {
                background-color: #ffffff;
                gridline-color: #dcdcdc;
                font-size: 12px;
            }
            QHeaderView::section {
                background-color: #f2f2f2;
                padding: 5px;
                border: 1px solid #cccccc;
                font-weight: bold;
            }
        """)

        list_layout.addWidget(self.servers_table)
        layout.addWidget(list_group)
        
    def on_server_selected(self):
        """Triggered when a server is selected from a combo box."""
        if not self.db_manager:
            return
                    
        server_text = self.station_server_combo.currentText().strip()
        if server_text:
            parts = server_text.split(" ", 1)
            if len(parts) == 2:
                username, host = parts
                self.selected_username = username
                self.selected_host = host
            else:
                servers = self.db_manager.get_servers()
                server = next((s for s in servers if s["username"] == server_text), None)
                if server:
                    self.selected_username = server["username"]
                    self.selected_host = server["host"]

    def create_station_settings_tab(self):
        """Create Station Settings sub-tab"""
        station_widget = QWidget()
        self.settings_tabs.addTab(station_widget, "Station Settings")

        layout = QHBoxLayout(station_widget)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(20)

        left_layout = QVBoxLayout()

        server_group = QGroupBox("Select Server")
        server_layout = QVBoxLayout(server_group)

        self.station_server_combo = QComboBox()
        self.station_server_combo.currentTextChanged.connect(self.load_stations_for_server)
        self.station_server_combo.currentTextChanged.connect(self.on_server_selected)
        server_layout.addWidget(self.station_server_combo)
        left_layout.addWidget(server_group)
        
        servers = self.db_manager.get_servers() if self.db_manager else []
        self.station_server_combo.clear()
        for server in servers:
            display_text = server["username"]
            self.station_server_combo.addItem(display_text, server)

        input_group = QGroupBox("Add Station")
        input_layout = QHBoxLayout(input_group)

        self.station_id_edit = QLineEdit()
        self.station_id_edit.setPlaceholderText("e.g., STATION01")
        self.add_station_btn = QPushButton("Add Station")
        self.add_station_btn.clicked.connect(self.add_station)

        input_layout.addWidget(QLabel("Station ID:"))
        input_layout.addWidget(self.station_id_edit)
        input_layout.addWidget(self.add_station_btn)

        left_layout.addWidget(input_group)
        left_layout.addStretch()
        layout.addLayout(left_layout, 1)

        station_list_group = QGroupBox("Station List")
        station_list_layout = QVBoxLayout(station_list_group)
        station_list_layout.setContentsMargins(10, 10, 10, 10)
        station_list_layout.setSpacing(10)

        self.stations_list = CheckboxListWidget("")
        self.stations_list.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        station_list_layout.addWidget(self.stations_list, stretch=1)

        self.delete_stations_btn = QPushButton("Delete Selected")
        self.delete_stations_btn.clicked.connect(self.delete_selected_stations)

        button_layout = QHBoxLayout()
        button_layout.addStretch()
        button_layout.addWidget(self.delete_stations_btn)
        station_list_layout.addLayout(button_layout)

        layout.addWidget(station_list_group, 2)
        station_widget.setLayout(layout)
        
    def create_select_server_tab(self):
        """Create Select Server sub-tab"""
        select_widget = QWidget()
        self.settings_tabs.addTab(select_widget, "Select Servers")
        
        layout = QHBoxLayout(select_widget)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(20)

        saved_group = QGroupBox("Saved Servers")
        saved_group_layout = QVBoxLayout(saved_group)
        saved_group_layout.setContentsMargins(10, 10, 10, 10)

        self.saved_servers_list = CheckboxListWidget("")
        saved_group_layout.addWidget(self.saved_servers_list)

        layout.addWidget(saved_group, 2)

        control_layout = QVBoxLayout() 
        control_layout.addStretch()

        self.add_servers_btn = QPushButton("Add →")
        self.remove_servers_btn = QPushButton("← Remove")

        self.add_servers_btn.setFixedWidth(120)
        self.remove_servers_btn.setFixedWidth(120)

        self.add_servers_btn.clicked.connect(self.add_servers_to_selected)
        self.remove_servers_btn.clicked.connect(self.remove_servers_from_selected)

        control_layout.addWidget(self.add_servers_btn)
        control_layout.addWidget(self.remove_servers_btn)
        control_layout.addStretch()
        
        layout.addLayout(control_layout)
        
        selected_group = QGroupBox("Selected Servers")
        selected_group_layout = QVBoxLayout(selected_group)
        selected_group_layout.setContentsMargins(10, 10, 10, 10)

        self.selected_servers_list = CheckboxListWidget("")
        selected_group_layout.addWidget(self.selected_servers_list)

        layout.addWidget(selected_group, 2)
        select_widget.setLayout(layout)
    
    def create_main_tab(self):
        """Create Main tab with server sub-tabs"""
        self.main_tab_widget = QWidget()
        self.main_tabs.addTab(self.main_tab_widget, "Main")
        
        layout = QVBoxLayout(self.main_tab_widget)
        
        self.server_tabs = QTabWidget()
        layout.addWidget(self.server_tabs)
        
        self.refresh_main_tabs()
    
    def create_server_main_tab(self, server):
        """Create main tab for a specific server"""
        if not self.db_manager:
            return QWidget()

        server_widget = ServerWidget()
        layout = QVBoxLayout(server_widget)

        stations_layout = QHBoxLayout()

        available_group = QGroupBox("Available Stations")
        available_layout = QVBoxLayout(available_group)

        available_stations = CheckboxListWidget("")
        stations = self.db_manager.get_stations(server['username'])
        for station in stations:
            if not station['is_selected']:
                available_stations.add_item(station['station_id'], station['station_id'])

        available_layout.addWidget(available_stations)
        stations_layout.addWidget(available_group)

        control_layout = QVBoxLayout()
        control_layout.addStretch()

        add_btn = QPushButton("Add →")
        remove_btn = QPushButton("← Remove")

        control_layout.addWidget(add_btn)
        control_layout.addWidget(remove_btn)
        control_layout.addStretch()

        stations_layout.addLayout(control_layout)

        selected_group = QGroupBox("Selected Stations")
        selected_layout = QVBoxLayout(selected_group)

        # Create a special list widget for selected stations (no checkboxes needed for download)
        selected_stations = CheckboxListWidget("")
        for station in stations:
            if station['is_selected']:
                selected_stations.add_item(station['station_id'], station['station_id'], False)  # Don't check by default

        selected_layout.addWidget(selected_stations)
        stations_layout.addWidget(selected_group)

        add_btn.clicked.connect(lambda: self.move_stations(server['username'], available_stations, selected_stations, True))
        remove_btn.clicked.connect(lambda: self.move_stations(server['username'], selected_stations, available_stations, False))

        layout.addLayout(stations_layout)

        settings_layout = QHBoxLayout()

        folder_group = QGroupBox("Local Folder")
        folder_layout = QHBoxLayout(folder_group)

        local_folder_edit = QLineEdit()
        username = server["username"]
        local_folder_edit.setText(self.db_manager.get_setting(f'server_{username}_local_folder'))
        browse_btn = QPushButton("Browse")
        browse_btn.clicked.connect(lambda: self.browse_folder(local_folder_edit, server['username']))

        folder_layout.addWidget(local_folder_edit)
        folder_layout.addWidget(browse_btn)
        settings_layout.addWidget(folder_group)

        date_group = QGroupBox("Date")
        date_layout = QHBoxLayout(date_group)

        start_date = QDateEdit()
        start_date.setDate(QDate.currentDate())
        start_date.setDisplayFormat("dd/MM/yyyy")
        start_date.setFixedWidth(100)
        start_date.setCalendarPopup(True)

        end_date = QDateEdit()
        end_date.setDate(QDate.currentDate())
        end_date.setDisplayFormat("dd/MM/yyyy")
        end_date.setFixedWidth(100)
        end_date.setCalendarPopup(True)

        date_layout.addWidget(QLabel("From:"))
        date_layout.addWidget(start_date)
        date_layout.addWidget(QLabel("To:"))
        date_layout.addWidget(end_date)
        settings_layout.addWidget(date_group)

        time_group = QGroupBox("Time")
        time_layout = QHBoxLayout(time_group)

        start_time = QTimeEdit()
        start_time.setDisplayFormat("HH:mm")
        start_time.setTime(QTime(0, 0))
        start_time.setButtonSymbols(QAbstractSpinBox.ButtonSymbols.NoButtons)
        start_time.setKeyboardTracking(True)
        start_time.setWrapping(True)
        start_time.setReadOnly(False)

        end_time = QTimeEdit()
        end_time.setDisplayFormat("HH:mm")
        end_time.setTime(QTime(23, 59))
        end_time.setButtonSymbols(QAbstractSpinBox.ButtonSymbols.NoButtons)
        end_time.setKeyboardTracking(True)
        end_time.setWrapping(True)
        end_time.setReadOnly(False)

        time_layout.addWidget(QLabel("From:"))
        time_layout.addWidget(start_time)
        time_layout.addWidget(QLabel("To:"))
        time_layout.addWidget(end_time)

        settings_layout.addWidget(time_group)
        layout.addLayout(settings_layout)

        auto_layout = QHBoxLayout()
        auto_download_checkbox = QCheckBox("Enable Auto Download at")
        
        # Add time picker for auto download
        auto_time_edit = QTimeEdit()
        auto_time_edit.setDisplayFormat("HH:mm")
        auto_time_edit.setTime(QTime(17, 0))  # Default 17:00
        auto_time_edit.setFixedWidth(70)
        auto_time_edit.setStyleSheet("""
            QTimeEdit {
                padding: 6px 8px;
                font-size: 13px;
                border: 1px solid #ccc;
                border-radius: 3px;
                background-color: white;
            }
            QTimeEdit:focus {
                border: 1px solid #4CAF50;
            }
            QTimeEdit::up-button, QTimeEdit::down-button {
                width: 0px;
                border: none;
            }
        """)

        auto_layout.addWidget(auto_download_checkbox)
        auto_layout.addWidget(auto_time_edit)
        auto_layout.addStretch()

        layout.addLayout(auto_layout)

        server_widget.auto_download_checkbox = auto_download_checkbox
        server_widget.auto_time_edit = auto_time_edit

        control_buttons_layout = QHBoxLayout()

        download_btn = QPushButton("Start Download")
        pause_btn = QPushButton("Pause")
        resume_btn = QPushButton("Resume")
        cancel_btn = QPushButton("Cancel")
        save_auto_btn = QPushButton("Save")

        control_buttons_layout.addWidget(download_btn)
        control_buttons_layout.addWidget(pause_btn)
        control_buttons_layout.addWidget(resume_btn)
        control_buttons_layout.addWidget(cancel_btn)
        control_buttons_layout.addWidget(save_auto_btn)
        control_buttons_layout.addStretch()

        layout.addLayout(control_buttons_layout)

        progress_bar = QProgressBar()
        progress_bar.setVisible(False)
        layout.addWidget(progress_bar)

        status_label = QLabel("Ready")
        layout.addWidget(status_label)

        download_btn.clicked.connect(
            lambda: self.start_download(server, local_folder_edit, start_date, end_date, start_time, end_time, selected_stations)
        )
        pause_btn.clicked.connect(lambda: self.pause_download(server['username']))
        resume_btn.clicked.connect(lambda: self.resume_download(server['username']))
        cancel_btn.clicked.connect(lambda: self.cancel_download(server['username']))
        save_auto_btn.clicked.connect(
            lambda: self.save_and_auto_download(server, local_folder_edit, start_date, end_date, start_time, end_time, selected_stations)
        )

        server_widget.local_folder_edit = local_folder_edit
        server_widget.start_date = start_date
        server_widget.end_date = end_date
        server_widget.start_time = start_time
        server_widget.end_time = end_time
        server_widget.selected_stations = selected_stations
        server_widget.progress_bar = progress_bar
        server_widget.status_label = status_label
        server_widget.auto_download_checkbox = auto_download_checkbox

        return server_widget
    
    def create_history_tab(self):
        """Create History tab"""
        history_widget = QWidget()
        self.main_tabs.addTab(history_widget, "History")
        
        layout = QVBoxLayout(history_widget)
        
        # Controls
        controls_layout = QHBoxLayout()
        
        refresh_btn = QPushButton("Refresh")
        clear_btn = QPushButton("Clear History")
        export_btn = QPushButton("Export History")
        
        # Add filter controls
        filter_label = QLabel("Show:")
        self.history_filter_combo = QComboBox()
        self.history_filter_combo.addItems(["Last 100", "Last 500", "Last 1000", "All"])
        self.history_filter_combo.setCurrentIndex(0)  # Default to Last 100
        self.history_filter_combo.currentTextChanged.connect(self.refresh_history)
        
        status_filter_label = QLabel("Status:")
        self.status_filter_combo = QComboBox()
        self.status_filter_combo.addItems(["All", "Success Only", "Failed Only"])
        self.status_filter_combo.currentTextChanged.connect(self.refresh_history)
        
        refresh_btn.clicked.connect(self.refresh_history)
        clear_btn.clicked.connect(self.clear_history)
        export_btn.clicked.connect(self.export_history)
        
        controls_layout.addWidget(refresh_btn)
        controls_layout.addWidget(clear_btn)
        controls_layout.addWidget(export_btn)
        controls_layout.addWidget(QLabel("  |  "))  # Separator
        controls_layout.addWidget(filter_label)
        controls_layout.addWidget(self.history_filter_combo)
        controls_layout.addWidget(status_filter_label)
        controls_layout.addWidget(self.status_filter_combo)
        controls_layout.addStretch()
        
        layout.addLayout(controls_layout)
        
        # Statistics bar
        self.history_stats_label = QLabel("Total: 0 | Success: 0 | Failed: 0")
        self.history_stats_label.setStyleSheet("""
            QLabel {
                background-color: #e8f5e9;
                padding: 8px;
                border-radius: 4px;
                font-weight: bold;
                color: #2e7d32;
            }
        """)
        layout.addWidget(self.history_stats_label)
        
        # History display
        self.history_text = QTextEdit()
        self.history_text.setReadOnly(True)
        self.history_text.setFont(QFont("Consolas", 9))
        self.history_text.setStyleSheet("""
            QTextEdit {
                background-color: #fafafa;
                border: 1px solid #ddd;
            }
        """)
        layout.addWidget(self.history_text)
    
    def add_server(self):
        """Add new server"""
        if not self.db_manager:
            return
            
        host = self.server_ip_edit.text().strip()
        port = self.server_port_edit.value()
        username = self.server_username_edit.text().strip()
        password = self.server_password_edit.text()
        remote_path = self.server_path_edit.text().strip() or "/"
        
        if not all([host, username, password]):
            QMessageBox.warning(self, "Warning", "Please fill in all required fields (IP, Username, Password)")
            return
        
        if self.db_manager.add_server(host, port, username, password, remote_path):
            self.clear_server_form()
            self.refresh_servers_table()
            self.refresh_all_data()
            self.log_activity(f"Added server: {host}:{port}")
        else:
            QMessageBox.critical(self, "Error", "Failed to add server. Check if server already exists.")
    
    def update_server(self):
        """Update existing server"""
        if not self.db_manager or not self.current_username:
            return
        
        host = self.server_ip_edit.text().strip()
        port = self.server_port_edit.value()
        username = self.server_username_edit.text().strip()
        password = self.server_password_edit.text()
        remote_path = self.server_path_edit.text().strip() or "/"
        
        if not all([host, username, password]):
            QMessageBox.warning(self, "Warning", "Please fill in all required fields")
            return
        
        if self.db_manager.update_server(self.current_username, host, port, password, remote_path):
            QMessageBox.information(self, "Success", "Server updated successfully!")
            self.clear_server_form()
            self.refresh_servers_table()
            self.refresh_all_data()
            self.log_activity(f"Updated server: {host}:{port}")
        else:
            QMessageBox.critical(self, "Error", "Failed to update server")
    
    def clear_server_form(self):
        """Clear server form"""
        self.server_ip_edit.clear()
        self.server_port_edit.setValue(21)
        self.server_username_edit.clear()
        self.server_password_edit.clear()
        self.server_path_edit.clear()
        self.add_server_btn.setEnabled(True)
        self.update_server_btn.setEnabled(False)
        self.current_username = None
    
    def refresh_servers_table(self):
        """Refresh servers table"""
        if not self.db_manager:
            return
            
        servers = self.db_manager.get_servers()
        self.servers_table.setRowCount(len(servers))
        
        for row, server in enumerate(servers):
            self.servers_table.setItem(row, 0, QTableWidgetItem(server['host']))
            self.servers_table.setItem(row, 1, QTableWidgetItem(server['username']))
            self.servers_table.setItem(row, 2, QTableWidgetItem(server.get('remote_path', '')))
            
            button_style = """
            QPushButton {
                background-color: #f2f2f2;
                border: 1px solid #cccccc;
                border-radius: 6px;
                padding: 4px 8px;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #e6e6e6;
                border-color: #999999;
            }
            QPushButton:pressed {
                background-color: #d9d9d9;
                border-color: #888888;
            }"""
            
            edit_btn = QPushButton("✏️")
            edit_btn.setStyleSheet(button_style)
            edit_btn.clicked.connect(lambda checked, s=server: self.edit_server(s))
            self.servers_table.setCellWidget(row, 3, edit_btn)
            
            delete_btn = QPushButton("🗑️")
            delete_btn.setStyleSheet(button_style)
            delete_btn.clicked.connect(lambda checked, s=server: self.delete_server(s))
            self.servers_table.setCellWidget(row, 4, delete_btn)
    
    def edit_server(self, server):
        """Edit server"""
        self.server_ip_edit.setText(server['host'])
        self.server_port_edit.setValue(server['port'])
        self.server_username_edit.setText(server['username'])
        self.server_password_edit.setText(server['password'])
        self.server_path_edit.setText(server.get('remote_path', ''))
        
        self.current_username = server['username']
        self.add_server_btn.setEnabled(False)
        self.update_server_btn.setEnabled(True)
        
        self.settings_tabs.setCurrentIndex(0)
    
    def delete_server(self, server):
        """Delete server"""
        if not self.db_manager:
            return
            
        reply = QMessageBox.question(self, "Confirm Delete", 
                                   f"Are you sure you want to delete server '{server['host']}:{server['port']}'?\n\nThis will also delete all associated stations and history.",
                                   QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        
        if reply == QMessageBox.StandardButton.Yes:
            if self.db_manager.delete_server(server['username']):
                QMessageBox.information(self, "Success", "Server deleted successfully!")
                self.refresh_servers_table()
                self.refresh_all_data()
                self.log_activity(f"Deleted server: {server['host']}:{server['port']}")
            else:
                QMessageBox.critical(self, "Error", "Failed to delete server")
    
    def test_connection(self):
        """Test FTP connection"""
        host = self.server_ip_edit.text().strip()
        port = self.server_port_edit.value()
        username = self.server_username_edit.text().strip()
        password = self.server_password_edit.text()
        
        if not all([host, username, password]):
            QMessageBox.warning(self, "Warning", "Please fill in all required fields")
            return
        
        QApplication.processEvents()
        
        try:
            success, message = test_ftp_connection(host, username, password, port)

            if success:
                QMessageBox.information(self, "Success", f"Successfully connected!")
                self.log_activity(f"Connection test successful for {username}:{host}")
            else:
                QMessageBox.critical(self, "Error", f"Connection test failed:\n{message}")
                self.log_activity(f"Connection test failed for {username}:{host} - {message}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Connection test error:\n{str(e)}")
            self.log_activity(f"Connection test error for {username}:{host} - {str(e)}")
    
    def preview_remote_directory(self):
        """Preview remote directory"""
        host = self.server_ip_edit.text().strip()
        port = self.server_port_edit.value()
        username = self.server_username_edit.text().strip()
        password = self.server_password_edit.text()
        remote_path = self.server_path_edit.text().strip() or "/"
        
        if not all([host, username, password]):
            QMessageBox.warning(self, "Warning", "Please fill in all required fields")
            return
        
        try:
            success, files, message = get_remote_directory_listing(host, username, password, remote_path, port)
            
            if success:
                dialog = QDialog(self)
                dialog.setWindowTitle("Remote Directory Preview")
                dialog.setModal(True)
                dialog.resize(600, 400)
                
                layout = QVBoxLayout()
                
                label = QLabel(f"Directory: {remote_path}")
                label.setFont(QFont("Arial", 10, QFont.Weight.Bold))
                layout.addWidget(label)
                
                text_edit = QTextEdit()
                text_edit.setReadOnly(True)
                text_edit.setFont(QFont("Consolas", 9))
                text_edit.setPlainText('\n'.join(files[:100]))
                layout.addWidget(text_edit)
                
                info_label = QLabel(f"Showing {min(len(files), 100)} of {len(files)} items")
                layout.addWidget(info_label)
                
                close_btn = QPushButton("Close")
                close_btn.clicked.connect(dialog.accept)
                layout.addWidget(close_btn)
                
                dialog.setLayout(layout)
                dialog.exec()
                
                self.log_activity(f"Previewed remote directory {remote_path} on {host}")
            else:
                QMessageBox.critical(self, "Error", f"Failed to preview directory:\n{message}")
                self.log_activity(f"Preview failed for {host}:{remote_path} - {message}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Preview error:\n{str(e)}")
            self.log_activity(f"Preview error for {host}:{remote_path} - {str(e)}")
            
    def load_stations_for_server(self):
        """Load stations for selected server"""
        if not self.db_manager:
            return
            
        server_text = self.station_server_combo.currentText()
        if not server_text:
            return
        
        servers = self.db_manager.get_servers()
        server = next((s for s in servers if s["username"] == server_text), None)

        if server:
            self.stations_list.clear_items()
            stations = self.db_manager.get_stations(server['username'])
            
            for station in stations:
                self.stations_list.add_item(station['station_id'], station['station_id'])
    
    def add_station(self):
        """Add new station"""
        if not self.db_manager:
            return
            
        server_text = self.station_server_combo.currentText()
        station_id = self.station_id_edit.text().strip().upper()
        
        if not server_text or not station_id:
            QMessageBox.warning(self, "Warning", "Please select server and enter station ID")
            return
        
        servers = self.db_manager.get_servers()
        server = next((s for s in servers if s["username"] == server_text), None)
        
        if server:
            if self.db_manager.add_station(station_id, server['username']):
                self.station_id_edit.clear()
                self.load_stations_for_server()
                self.refresh_main_tabs()
                self.log_activity(f"Added station {station_id} to server {server_text}")
            else:
                QMessageBox.warning(self, "Warning", f"Station '{station_id}' already exists or failed to add")
    
    def delete_selected_stations(self):
        """Delete selected stations"""
        if not self.db_manager:
            return
            
        selected_data = self.stations_list.get_checked_data()
        
        if not selected_data:
            QMessageBox.warning(self, "Warning", "Please select stations to delete")
            return
        
        reply = QMessageBox.question(self, "Confirm Delete", 
                                   f"Are you sure you want to delete {len(selected_data)} station(s)?",
                                   QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        
        if reply == QMessageBox.StandardButton.Yes:
            selected_username = self.selected_username
            success_count = 0
            for station_id in selected_data:
                if selected_username:
                    if self.db_manager.delete_station(station_id, selected_username):
                        success_count += 1
                else:
                    QMessageBox.warning(self, "Error", "No user selected.")
                    break
            
            QMessageBox.information(self, "Success", f"Deleted {success_count} station(s)")
            self.load_stations_for_server()
            self.refresh_main_tabs()
            self.log_activity(f"Deleted {success_count} stations")
    
    def add_servers_to_selected(self):
        """Add servers to selected list"""
        if not self.db_manager:
            return
            
        selected_data = self.saved_servers_list.get_checked_data()
        
        if not selected_data:
            QMessageBox.warning(self, "Warning", "Please select servers to add")
            return
        
        for username in selected_data:
            self.db_manager.update_server_selection(username, True)
        
        self.refresh_server_selection_lists()
        self.refresh_main_tabs()
        self.log_activity(f"Added {len(selected_data)} servers to selection")
    
    def remove_servers_from_selected(self):
        """Remove servers from selected list"""
        if not self.db_manager:
            return
            
        selected_data = self.selected_servers_list.get_checked_data()
        
        if not selected_data:
            QMessageBox.warning(self, "Warning", "Please select servers to remove")
            return
        
        for username in selected_data:
            self.db_manager.update_server_selection(username, False)
        
        self.refresh_server_selection_lists()
        self.refresh_main_tabs()
        self.log_activity(f"Removed {len(selected_data)} servers from selection")
    
    def refresh_server_selection_lists(self):
        """Refresh server selection lists"""
        if not self.db_manager:
            return
            
        servers = self.db_manager.get_servers()
        
        self.saved_servers_list.clear_items()
        self.selected_servers_list.clear_items()
        
        for server in servers:
            server_display = f"{server['username']}"
            
            if server['is_selected']:
                self.selected_servers_list.add_item(server_display, server['username'])
            else:
                self.saved_servers_list.add_item(server_display, server['username'])
    
    def refresh_main_tabs(self):
        """Refresh main tabs based on selected servers"""
        if not self.db_manager:
            return
            
        self.server_tabs.clear()
        
        servers = self.db_manager.get_servers()
        selected_servers = [s for s in servers if s['is_selected']]
        
        if not selected_servers:
            placeholder = QLabel("No servers selected.\n\nGo to Settings > Select Servers to choose servers for download.")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            placeholder.setStyleSheet("color: #666; font-size: 14px;")
            self.server_tabs.addTab(placeholder, "No Servers")
        else:
            for server in selected_servers:
                server_widget = self.create_server_main_tab(server)
                tab_name = f"{server['username']}"
                self.server_tabs.addTab(server_widget, tab_name)
    
    def move_stations(self, username, from_list, to_list, is_selected):
        """Move stations between lists"""
        if not self.db_manager:
            return
            
        selected_data = from_list.get_checked_data()
        
        if not selected_data:
            QMessageBox.warning(self, "Warning", "Please select stations to move")
            return
        
        for station_id in selected_data:
            self.db_manager.update_station_selection(station_id, username, is_selected)
        
        stations = self.db_manager.get_stations(username)
        
        from_list.clear_items()
        to_list.clear_items()
        
        for station in stations:
            if station['is_selected'] == is_selected:
                to_list.add_item(station['station_id'], station['station_id'], True)
            else:
                from_list.add_item(station['station_id'], station['station_id'])
    
    def browse_folder(self, folder_edit, username):
        """Browse for local folder"""
        if not self.db_manager:
            return
            
        folder = QFileDialog.getExistingDirectory(self, "Select Download Folder")
        if folder:
            folder_edit.setText(folder)
            self.db_manager.set_setting(f'server_{username}_local_folder', folder)
    

    def start_download(self, server, local_folder_edit, start_date, end_date, start_time, end_time, selected_stations):
        """Start download for server"""
        if not self.db_manager:
            return
        
        username = server['username']
        
        # Clean up any existing worker/thread safely
        self.safe_cleanup_worker(username)
        
        # Check if download is still running after cleanup
        if username in self.download_workers:
            worker = self.download_workers[username]
            if worker.is_running:
                QMessageBox.warning(self, "Download In Progress", 
                                f"A download is already running for {username}. Please wait or cancel it first.")
                return
        
        # Get ALL stations from the Selected Stations list
        all_selected_items = [(item.text(), item.property("data")) for item in selected_stations.items]
        selected_station_data = [data for text, data in all_selected_items if data is not None]
        
        if not selected_station_data:
            QMessageBox.warning(self, "Warning", "No stations in Selected Stations list.\n\nPlease add stations using the 'Add →' button.")
            return
        
        local_folder = local_folder_edit.text().strip()
        if not local_folder:
            QMessageBox.warning(self, "Warning", "Please select local folder")
            return
        
        start_py_date = start_date.date().toPyDate()
        end_py_date = end_date.date().toPyDate()
        
        if start_py_date > end_py_date:
            QMessageBox.warning(self, "Warning", "Start date cannot be after end date")
            return
        
        params = {
            'start_dt': datetime.combine(start_py_date, start_time.time().toPyTime()),
            'end_dt': datetime.combine(end_py_date, end_time.time().toPyTime()),
            'local_folder': local_folder
        }
        
        # Show progress bar and reset status
        server_widget = self.get_server_widget(username)
        if server_widget:
            if server_widget.progress_bar:
                server_widget.progress_bar.setVisible(True)
                server_widget.progress_bar.setValue(0)
            if server_widget.status_label:
                server_widget.status_label.setText("Starting download...")

        
        # Create new worker and thread
        worker = DownloadWorker(server, selected_station_data, params, self.db_manager)
        thread = QThread()
        
        worker.progress_updated.connect(self.update_progress)
        worker.finished.connect(self.download_finished)
        worker.log_message.connect(self.log_activity)
        
        def cleanup_thread():
            """Clean up thread after worker finishes"""
            thread.quit()
            thread.wait()
            self.log_activity(f"Thread cleanup completed for {username}")
        
        worker.finished.connect(cleanup_thread)
        
        worker.moveToThread(thread)
        worker.set_thread(thread)
        thread.started.connect(worker.run)
        
        self.download_workers[username] = worker
        self.download_threads[username] = thread
        
        thread.start()
        
        self.log_activity(f"Started download for server {username} with {len(selected_station_data)} stations")
            
    def pause_download(self, username):
        """Pause download"""
        if username in self.download_workers:
            self.download_workers[username].pause()
    
    def resume_download(self, username):
        """Resume download"""
        if username in self.download_workers:
            self.download_workers[username].resume()
    
    def cancel_download(self, username):
        """Cancel download"""
        if username in self.download_workers:
            self.download_workers[username].cancel()
    
    def save_and_auto_download(self, server, local_folder_edit, start_date, end_date, start_time, end_time, selected_stations):
        """Save settings and set up optional auto download schedule"""
        if not self.db_manager:
            return

        username = server["username"]
        
        # ✅ Save settings to database
        settings = {
            "local_folder": local_folder_edit.text(),
            "start_date": start_date.date().toString("yyyy-MM-dd"),
            "end_date": end_date.date().toString("yyyy-MM-dd"),
            "start_time": start_time.time().toString("HH:mm"),
            "end_time": end_time.time().toString("HH:mm"),
            "selected_stations": [item.property("data") for item in selected_stations.items if item.property("data")],
        }

        self.db_manager.set_setting(f"server_{username}_auto_settings", json.dumps(settings))
        self.log_activity(f"Settings saved for server {username}")

        server_widget = self.get_server_widget(username)
        if not server_widget:
            QMessageBox.warning(self, "Error", "Could not find server widget")
            return

        # ✅ Check if auto-download is enabled
        if hasattr(server_widget, "auto_download_checkbox") and server_widget.auto_download_checkbox.isChecked():
            scheduled_time = server_widget.auto_time_edit.time()
            now = QTime.currentTime()
            ms_until_run = now.msecsTo(scheduled_time)

            # If time has passed today, schedule for tomorrow
            if ms_until_run < 0:
                ms_until_run += 24 * 60 * 60 * 1000

            # Initialize auto_timers dict if not exists
            if not hasattr(self, "auto_timers"):
                self.auto_timers = {}
            
            # Cancel existing timer if any
            if username in self.auto_timers:
                old_timer = self.auto_timers[username]
                if old_timer.isActive():
                    old_timer.stop()
                    self.log_activity(f"Cancelled previous auto-download timer for {username}")

            # Create new timer
            timer = QTimer(self)
            timer.setSingleShot(True)
            timer.timeout.connect(lambda: self.start_download(
                server,
                local_folder_edit,
                start_date,
                end_date,
                start_time,
                end_time,
                selected_stations
            ))

            timer.start(ms_until_run)
            self.auto_timers[username] = timer

            scheduled_str = scheduled_time.toString("HH:mm")
            self.log_activity(f"Auto download scheduled for {username} at {scheduled_str}")
            QMessageBox.information(
                self,
                "Settings Saved",
                f"✅ Settings saved successfully!\n\n📅 Auto download scheduled for {username} at {scheduled_str}"
            )
        else:
            # Auto-download is NOT enabled - just show success message
            QMessageBox.information(
                self,
                "Settings Saved",
                f"✅ Settings saved successfully for {username}!\n\nAuto-download is disabled."
            )
    

    def update_progress(self, server_info, status, total, downloaded, failed, current_file):
            """Update progress display - NO PROGRESS BAR, just status text"""
            for i in range(self.server_tabs.count()):
                if self.server_tabs.tabText(i) == server_info:
                    widget = self.server_tabs.widget(i)
                    if isinstance(widget, ServerWidget):
                        # Update status label only (no progress bar)
                        if widget.status_label:
                            # Show detailed status
                            status_text = f"{status}"
                            if downloaded > 0 or failed > 0:
                                status_text += f" | ✅ {downloaded} | ❌ {failed}"
                            if current_file and current_file != "batch":
                                # Truncate long filenames
                                display_file = current_file if len(current_file) < 40 else current_file[:37] + "..."
                                status_text += f" | {display_file}"
                            widget.status_label.setText(status_text)
                    break

    def download_finished(self, server_info, downloaded, failed):
        """Handle download completion with detailed options"""
        
        # Update status label
        for i in range(self.server_tabs.count()):
            if self.server_tabs.tabText(i) == server_info:
                widget = self.server_tabs.widget(i)
                if isinstance(widget, ServerWidget):
                    if widget.status_label:
                        # ✅ FIX: Better status messages
                        if downloaded == 0 and failed == 0:
                            widget.status_label.setText("✅ All files already exist - no new downloads")
                        elif failed > 0:
                            widget.status_label.setText(f"⚠️ Completed with {failed} failures")
                        else:
                            widget.status_label.setText("✅ Download completed successfully")
                    
                    if widget.progress_bar:
                        widget.progress_bar.setVisible(False)
                        widget.progress_bar.setValue(0)
                    break

        # ✅ FIX: Don't show dialog if no files were processed
        if downloaded == 0 and failed == 0:
            # All files already existed - show simple success message
            QMessageBox.information(
                self,
                "No New Files",
                f"✅ Download completed for {server_info}\n\n"
                f"All files already exist locally.\n\n"
                f"No new files were downloaded."
            )
            self.log_activity(f"Download completed for {server_info}: All files already exist")
            return
        
        # ✅ NEW: Check if all "failures" are actually empty files on server
        try:
            log_file = "download_log.json"
            empty_files_count = 0
            
            if os.path.exists(log_file):
                with open(log_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                # Count recent empty file warnings for this server
                recent_failures = [
                    e for e in data[-100:]  # Check last 100 entries
                    if e.get("username") == server_info 
                    and e.get("status") == "failed"
                    and "0 bytes" in e.get("message", "").lower()
                ]
                empty_files_count = len(recent_failures)
        except:
            empty_files_count = 0
        
        # Create custom dialog (only if there were actual downloads or failures)
        dialog = QDialog(self)
        dialog.setWindowTitle("Download Complete")
        dialog.setModal(True)
        dialog.setMinimumWidth(550)
        
        layout = QVBoxLayout(dialog)
        
        # Header with icon and title
        header_layout = QHBoxLayout()   
        
        # Title and stats
        info_layout = QVBoxLayout()
        title_label = QLabel(f"Download completed for {server_info}")
        title_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        info_layout.addWidget(title_label)
        
        # ✅ IMPROVED: Show breakdown of failures
        stats_text = f"✅ {downloaded} files downloaded"
        if failed > 0:
            stats_text += f"\n❌ {failed} files failed"
            if empty_files_count > 0:
                stats_text += f"\n   ⚠️ {empty_files_count} were empty on server (0 bytes)"
                real_failures = failed - empty_files_count
                if real_failures > 0:
                    stats_text += f"\n   ❌ {real_failures} actual download errors"
        
        stats_label = QLabel(stats_text)
        stats_label.setStyleSheet("font-size: 12px; color: #666;")
        info_layout.addWidget(stats_label)
        
        header_layout.addLayout(info_layout)
        header_layout.addStretch()
        layout.addLayout(header_layout)
        
        # Separator
        line = QLabel()
        line.setStyleSheet("background-color: #ddd; max-height: 1px;")
        layout.addWidget(line)
        
        # ✅ Info box for empty files
        if empty_files_count > 0:
            info_box = QLabel(
                f"ℹ️ Note: {empty_files_count} file(s) are empty (0 bytes) on the FTP server.\n"
                f"These files cannot be downloaded. Contact the server administrator if this is unexpected."
            )
            info_box.setStyleSheet("""
                QLabel {
                    background-color: #fff3cd;
                    border: 1px solid #ffc107;
                    border-radius: 4px;
                    padding: 10px;
                    color: #856404;
                }
            """)
            info_box.setWordWrap(True)
            layout.addWidget(info_box)
        
        # Options section (only show if there are failures)
        real_failures = failed - empty_files_count if empty_files_count > 0 else failed
        
        if real_failures > 0:
            options_group = QGroupBox("What would you like to do?")
            options_layout = QVBoxLayout(options_group)
            
            # View failed files button
            view_failed_btn = QPushButton("📋 View Failed Files")
            view_failed_btn.setStyleSheet("""
                QPushButton {
                    background-color: #2196F3;
                    color: white;
                    padding: 10px;
                    text-align: left;
                    border-radius: 4px;
                }
                QPushButton:hover {
                    background-color: #1976D2;
                }
            """)
            view_failed_btn.clicked.connect(lambda: self.show_failed_files(server_info))
            options_layout.addWidget(view_failed_btn)
            
            # Retry failed files button
            retry_btn = QPushButton("🔄 Retry Failed Files")
            retry_btn.setStyleSheet("""
                QPushButton {
                    background-color: #FF9800;
                    color: white;
                    padding: 10px;
                    text-align: left;
                    border-radius: 4px;
                }
                QPushButton:hover {
                    background-color: #F57C00;
                }
            """)
            retry_btn.clicked.connect(lambda: [dialog.accept(), self.retry_failed_files(server_info)])
            options_layout.addWidget(retry_btn)
            
            # Export failed list button
            export_btn = QPushButton("💾 Export Failed Files List")
            export_btn.setStyleSheet("""
                QPushButton {
                    background-color: #9C27B0;
                    color: white;
                    padding: 10px;
                    text-align: left;
                    border-radius: 4px;
                }
                QPushButton:hover {
                    background-color: #7B1FA2;
                }
            """)
            export_btn.clicked.connect(lambda: self.export_failed_files(server_info))
            options_layout.addWidget(export_btn)
            
            layout.addWidget(options_group)
        elif failed > 0 and empty_files_count == failed:
            # All failures are empty files - show informational message
            info_label = QLabel(
                "All failed files are empty on the FTP server.\n"
                "No files can be retried until they contain data."
            )
            info_label.setStyleSheet("color: #666; font-style: italic; padding: 10px;")
            info_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(info_label)
        
        # Bottom buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        view_history_btn = QPushButton("📜 View History")
        view_history_btn.clicked.connect(lambda: [dialog.accept(), self.main_tabs.setCurrentIndex(2)])
        button_layout.addWidget(view_history_btn)
        
        ok_btn = QPushButton("OK")
        ok_btn.setDefault(True)
        ok_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                padding: 8px 24px;
                min-width: 80px;
            }
        """)
        ok_btn.clicked.connect(dialog.accept)
        button_layout.addWidget(ok_btn)
        
        layout.addLayout(button_layout)
        
        dialog.setLayout(layout)
        dialog.exec()
        
        # Log completion
        self.log_activity(f"Download finished for {server_info}: {downloaded} success, {failed} failed")
        """Handle download completion with detailed options"""
        
        # Update status label
        for i in range(self.server_tabs.count()):
            if self.server_tabs.tabText(i) == server_info:
                widget = self.server_tabs.widget(i)
                if isinstance(widget, ServerWidget):
                    if widget.status_label:
                        # ✅ FIX: Better status messages
                        if downloaded == 0 and failed == 0:
                            widget.status_label.setText("✅ All files already exist - no new downloads")
                        elif failed > 0:
                            widget.status_label.setText(f"⚠️ Completed with {failed} failures")
                        else:
                            widget.status_label.setText("✅ Download completed successfully")
                    
                    if widget.progress_bar:
                        widget.progress_bar.setVisible(False)
                        widget.progress_bar.setValue(0)
                    break

        # ✅ FIX: Don't show dialog if no files were processed
        if downloaded == 0 and failed == 0:
            # All files already existed - show simple success message
            QMessageBox.information(
                self,
                "No New Files",
                f"✅ Download completed for {server_info}\n\n"
                f"All files already exist locally.\n\n"
                f"No new files were downloaded."
            )
            self.log_activity(f"Download completed for {server_info}: All files already exist")
            return
        
        # Create custom dialog (only if there were actual downloads or failures)
        dialog = QDialog(self)
        dialog.setWindowTitle("Download Complete")
        dialog.setModal(True)
        dialog.setMinimumWidth(500)
        
        layout = QVBoxLayout(dialog)
        
        # Header with icon and title
        header_layout = QHBoxLayout()   
        
        # Title and stats
        info_layout = QVBoxLayout()
        title_label = QLabel(f"Download completed for {server_info}")
        title_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        info_layout.addWidget(title_label)
        
        stats_label = QLabel(f"✅ {downloaded} files downloaded\n❌ {failed} files failed")
        stats_label.setStyleSheet("font-size: 12px; color: #666;")
        info_layout.addWidget(stats_label)
        
        header_layout.addLayout(info_layout)
        header_layout.addStretch()
        layout.addLayout(header_layout)
        
        # Separator
        line = QLabel()
        line.setStyleSheet("background-color: #ddd; max-height: 1px;")
        layout.addWidget(line)
        
        # Options section (only show if there are failures)
        if failed > 0:
            options_group = QGroupBox("What would you like to do?")
            options_layout = QVBoxLayout(options_group)
            
            # View failed files button
            view_failed_btn = QPushButton("📋 View Failed Files")
            view_failed_btn.setStyleSheet("""
                QPushButton {
                    background-color: #2196F3;
                    color: white;
                    padding: 10px;
                    text-align: left;
                    border-radius: 4px;
                }
                QPushButton:hover {
                    background-color: #1976D2;
                }
            """)
            view_failed_btn.clicked.connect(lambda: self.show_failed_files(server_info))
            options_layout.addWidget(view_failed_btn)
            
            # Retry failed files button
            retry_btn = QPushButton("🔄 Retry Failed Files")
            retry_btn.setStyleSheet("""
                QPushButton {
                    background-color: #FF9800;
                    color: white;
                    padding: 10px;
                    text-align: left;
                    border-radius: 4px;
                }
                QPushButton:hover {
                    background-color: #F57C00;
                }
            """)
            retry_btn.clicked.connect(lambda: [dialog.accept(), self.retry_failed_files(server_info)])
            options_layout.addWidget(retry_btn)
            
            # Export failed list button
            export_btn = QPushButton("💾 Export Failed Files List")
            export_btn.setStyleSheet("""
                QPushButton {
                    background-color: #9C27B0;
                    color: white;
                    padding: 10px;
                    text-align: left;
                    border-radius: 4px;
                }
                QPushButton:hover {
                    background-color: #7B1FA2;
                }
            """)
            export_btn.clicked.connect(lambda: self.export_failed_files(server_info))
            options_layout.addWidget(export_btn)
            
            layout.addWidget(options_group)
        
        # Bottom buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        view_history_btn = QPushButton("📜 View History")
        view_history_btn.clicked.connect(lambda: [dialog.accept(), self.main_tabs.setCurrentIndex(2)])
        button_layout.addWidget(view_history_btn)
        
        ok_btn = QPushButton("OK")
        ok_btn.setDefault(True)
        ok_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                padding: 8px 24px;
                min-width: 80px;
            }
        """)
        ok_btn.clicked.connect(dialog.accept)
        button_layout.addWidget(ok_btn)
        
        layout.addLayout(button_layout)
        
        dialog.setLayout(layout)
        dialog.exec()
        
        # Log completion
        self.log_activity(f"Download finished for {server_info}: {downloaded} success, {failed} failed")
        
        # Update status label
        for i in range(self.server_tabs.count()):
            if self.server_tabs.tabText(i) == server_info:
                widget = self.server_tabs.widget(i)
                if isinstance(widget, ServerWidget):
                    if widget.status_label:
                        # ✅ FIX: Better status messages
                        if downloaded == 0 and failed == 0:
                            widget.status_label.setText("✅ All files already exist - no new downloads")
                        elif failed > 0:
                            widget.status_label.setText(f"⚠️ Completed with {failed} failures")
                        else:
                            widget.status_label.setText("✅ Download completed successfully")
                    
                    if widget.progress_bar:
                        widget.progress_bar.setVisible(False)
                        widget.progress_bar.setValue(0)
                    break

        # ✅ FIX: Don't show dialog if no files were processed
        if downloaded == 0 and failed == 0:
            # All files already existed - show simple success message
            QMessageBox.information(
                self,
                "No New Files",
                f"✅ Download completed for {server_info}\n\n"
                f"All files already exist locally.\n\n"
                f"No new files were downloaded."
            )
            self.log_activity(f"Download completed for {server_info}: All files already exist")
            return
        
        # Create custom dialog (only if there were actual downloads or failures)
        dialog = QDialog(self)
        dialog.setWindowTitle("Download Complete")
        dialog.setModal(True)
        dialog.setMinimumWidth(500)
        
        layout = QVBoxLayout(dialog)
        
        # Header with icon and title
        header_layout = QHBoxLayout()   
        
        # Title and stats
        info_layout = QVBoxLayout()
        title_label = QLabel(f"Download completed for {server_info}")
        title_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        info_layout.addWidget(title_label)
        
        stats_label = QLabel(f"✅ {downloaded} files downloaded\n❌ {failed} files failed")
        stats_label.setStyleSheet("font-size: 12px; color: #666;")
        info_layout.addWidget(stats_label)
        
        header_layout.addLayout(info_layout)
        header_layout.addStretch()
        layout.addLayout(header_layout)
        
        # Separator
        line = QLabel()
        line.setStyleSheet("background-color: #ddd; max-height: 1px;")
        layout.addWidget(line)
        
        # Options section (only show if there are failures)
        if failed > 0:
            options_group = QGroupBox("What would you like to do?")
            options_layout = QVBoxLayout(options_group)
            
            # View failed files button
            view_failed_btn = QPushButton("📋 View Failed Files")
            view_failed_btn.setStyleSheet("""
                QPushButton {
                    background-color: #2196F3;
                    color: white;
                    padding: 10px;
                    text-align: left;
                    border-radius: 4px;
                }
                QPushButton:hover {
                    background-color: #1976D2;
                }
            """)
            view_failed_btn.clicked.connect(lambda: self.show_failed_files(server_info))
            options_layout.addWidget(view_failed_btn)
            
            # Retry failed files button
            retry_btn = QPushButton("🔄 Retry Failed Files")
            retry_btn.setStyleSheet("""
                QPushButton {
                    background-color: #FF9800;
                    color: white;
                    padding: 10px;
                    text-align: left;
                    border-radius: 4px;
                }
                QPushButton:hover {
                    background-color: #F57C00;
                }
            """)
            retry_btn.clicked.connect(lambda: [dialog.accept(), self.retry_failed_files(server_info)])
            options_layout.addWidget(retry_btn)
            
            # Export failed list button
            export_btn = QPushButton("💾 Export Failed Files List")
            export_btn.setStyleSheet("""
                QPushButton {
                    background-color: #9C27B0;
                    color: white;
                    padding: 10px;
                    text-align: left;
                    border-radius: 4px;
                }
                QPushButton:hover {
                    background-color: #7B1FA2;
                }
            """)
            export_btn.clicked.connect(lambda: self.export_failed_files(server_info))
            options_layout.addWidget(export_btn)
            
            layout.addWidget(options_group)
        
        # Bottom buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        view_history_btn = QPushButton("📜 View History")
        view_history_btn.clicked.connect(lambda: [dialog.accept(), self.main_tabs.setCurrentIndex(2)])
        button_layout.addWidget(view_history_btn)
        
        ok_btn = QPushButton("OK")
        ok_btn.setDefault(True)
        ok_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                padding: 8px 24px;
                min-width: 80px;
            }
        """)
        ok_btn.clicked.connect(dialog.accept)
        button_layout.addWidget(ok_btn)
        
        layout.addLayout(button_layout)
        
        dialog.setLayout(layout)
        dialog.exec()
        
        # Log completion
        self.log_activity(f"Download finished for {server_info}: {downloaded} success, {failed} failed")

    def show_failed_files(self, server_info):
        """Show list of failed files from history - FIXED to show unique failed files"""
        try:
            log_file = "download_log.json"
            
            if not os.path.exists(log_file):
                QMessageBox.information(self, "No Data", "No download history found.")
                return
            
            with open(log_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # ✅ FIX: Get UNIQUE failed files (most recent status only)
            file_status = {}  # {(station, filename): (status, timestamp, entry)}
            
            for entry in data:
                if entry.get("username") == server_info:
                    station = entry.get("station_id", "")
                    filename = entry.get("filename", "")
                    status = entry.get("status", "").lower()
                    timestamp = entry.get("timestamp", "")
                    
                    key = (station, filename)
                    
                    # Keep only the most recent entry for each file
                    if key not in file_status or timestamp > file_status[key][1]:
                        file_status[key] = (status, timestamp, entry)
            
            # Filter for files that are CURRENTLY failed (most recent status is "failed")
            failed_entries = []
            for key, (status, timestamp, entry) in file_status.items():
                if status == "failed":
                    failed_entries.append(entry)
            
            if not failed_entries:
                QMessageBox.information(self, "No Failures", f"No failed files found for {server_info}\n\nAll downloads were successful!")
                return
            
            # Create dialog to show failed files
            dialog = QDialog(self)
            dialog.setWindowTitle(f"Failed Files - {server_info}")
            dialog.setModal(True)
            dialog.resize(700, 500)
            
            layout = QVBoxLayout(dialog)
            
            info_label = QLabel(f"Found {len(failed_entries)} unique failed files:")
            info_label.setStyleSheet("font-weight: bold; font-size: 12px; color: #d32f2f;")
            layout.addWidget(info_label)
            
            # Table to show failed files
            table = QTableWidget()
            table.setColumnCount(4)
            table.setHorizontalHeaderLabels(["Timestamp", "Station", "Filename", "Error Message"])
            table.setRowCount(len(failed_entries))
            table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
            table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
            
            # Sort by timestamp (most recent first)
            failed_entries.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            
            for row, entry in enumerate(failed_entries):
                table.setItem(row, 0, QTableWidgetItem(entry.get("timestamp", "N/A")))
                table.setItem(row, 1, QTableWidgetItem(entry.get("station_id", "N/A")))
                table.setItem(row, 2, QTableWidgetItem(entry.get("filename", "N/A")))
                table.setItem(row, 3, QTableWidgetItem(entry.get("message", "N/A")))
            
            # Auto-resize columns
            header = table.horizontalHeader()
            if header:
                header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
                header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
                header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
                header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
            
            layout.addWidget(table)
            
            # Close button
            close_btn = QPushButton("Close")
            close_btn.clicked.connect(dialog.accept)
            layout.addWidget(close_btn)
            
            dialog.setLayout(layout)
            dialog.exec()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load failed files:\n{str(e)}")

    def retry_failed_files(self, server_info):
        """Retry downloading failed files - improved logic with proper counting"""
        try:
            log_file = "download_log.json"
            
            if not os.path.exists(log_file):
                QMessageBox.warning(self, "No Data", "No download history found.")
                return
            
            # Read log and get failed files
            with open(log_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Get UNIQUE failed files (only keep the most recent status per file)
            file_status = {}  # {(station, filename): (status, timestamp, entry)}
            
            for entry in data:
                if entry.get("username") == server_info:
                    station = entry.get("station_id", "")
                    filename = entry.get("filename", "")
                    status = entry.get("status", "").lower()
                    timestamp = entry.get("timestamp", "")
                    
                    key = (station, filename)
                    
                    # Keep only the most recent entry for each file
                    if key not in file_status or timestamp > file_status[key][1]:
                        file_status[key] = (status, timestamp, entry)
            
            # ✅ FIX: Filter for files that are CURRENTLY failed (most recent status is "failed")
            # AND that don't exist locally yet
            failed_files = {}
            actually_failed_count = 0
            already_exists_count = 0
            
            for key, (status, timestamp, entry) in file_status.items():
                station, filename = key
                
                # Only include if most recent status is "failed"
                if status == "failed":
                    # ✅ CHECK: Does this file actually exist locally now?
                    # Get server configuration to find local path
                    servers = self.db_manager.get_servers() if self.db_manager else []
                    server = next((s for s in servers if s["username"] == server_info), None)
                    
                    if server:
                        server_widget = self.get_server_widget(server_info)
                        if server_widget and server_widget.local_folder_edit:
                            local_folder = server_widget.local_folder_edit.text().strip()
                            
                            # Check if file exists in the local folder structure
                            # Try multiple possible locations
                            file_exists = False
                            
                            # Get date range from widget (for folder structure)
                            if server_widget.start_date and server_widget.end_date:
                                start_str = server_widget.start_date.date().toPyDate().strftime("%d%m%Y")
                                end_str = server_widget.end_date.date().toPyDate().strftime("%d%m%Y")
                                
                                # Check: local_folder/station/date_range/filename
                                check_path = os.path.join(local_folder, station, f"{start_str}_{end_str}", filename)
                                if os.path.exists(check_path) and os.path.getsize(check_path) > 0:
                                    file_exists = True
                                    logger.debug(f"File now exists: {filename}")
                            
                            # If file exists now, don't count it as failed
                            if file_exists:
                                already_exists_count += 1
                                continue
                    
                    # File is still failed and doesn't exist
                    if station not in failed_files:
                        failed_files[station] = []
                    failed_files[station].append(filename)
                    actually_failed_count += 1
            
            if not failed_files:
                msg = f"No failed files found for {server_info}.\n\n"
                if already_exists_count > 0:
                    msg += f"ℹ️ Note: {already_exists_count} previously failed files now exist locally and were excluded."
                else:
                    msg += "All downloads were successful!"
                
                QMessageBox.information(self, "No Failed Files", msg)
                return
            
            total_failed = sum(len(files) for files in failed_files.values())
            
            # Show detailed breakdown
            details = "\n".join([f"  • {station}: {len(files)} files" for station, files in failed_files.items()])
            
            info_msg = f"Found {total_failed} failed file(s) across {len(failed_files)} station(s):\n\n{details}\n\n"
            
            if already_exists_count > 0:
                info_msg += f"ℹ️ {already_exists_count} previously failed files now exist and were excluded.\n\n"
            
            info_msg += "Do you want to retry downloading these files?\n\n"
            info_msg += "Note: Files that were successfully downloaded will be skipped automatically."
            
            reply = QMessageBox.question(
                self,
                "Retry Failed Files",
                info_msg,
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply != QMessageBox.StandardButton.Yes:
                return
            
            # Get server configuration
            servers = self.db_manager.get_servers() if self.db_manager else []
            server = next((s for s in servers if s["username"] == server_info), None)
            
            if not server:
                QMessageBox.critical(self, "Error", f"Server '{server_info}' not found.")
                return
            
            # Get server widget
            server_widget = self.get_server_widget(server_info)
            if not server_widget:
                QMessageBox.critical(self, "Error", "Could not find server widget.")
                return
            
            # Check if download is already running
            if server_info in self.download_workers:
                worker = self.download_workers[server_info]
                if worker.is_running:
                    QMessageBox.warning(
                        self,
                        "Download In Progress",
                        f"A download is already running for {server_info}.\n\n"
                        "Please wait for it to complete or cancel it first."
                    )
                    return
            
            # Get local folder
            local_folder = server_widget.local_folder_edit.text().strip()
            if not local_folder:
                QMessageBox.warning(self, "Warning", "Please configure local folder first.")
                return
            
            # Get list of stations to retry
            stations_to_retry = list(failed_files.keys())
            
            # Use current date/time settings from widget
            start_date = server_widget.start_date.date().toPyDate()
            end_date = server_widget.end_date.date().toPyDate()
            start_time = server_widget.start_time.time().toPyTime()
            end_time = server_widget.end_time.time().toPyTime()
            
            params = {
                'start_dt': datetime.combine(start_date, start_time),
                'end_dt': datetime.combine(end_date, end_time),
                'local_folder': local_folder
            }
            
            # Clean up any existing worker
            self.safe_cleanup_worker(server_info)
            
            # Create new worker for retry
            worker = DownloadWorker(server, stations_to_retry, params, self.db_manager)
            thread = QThread()
            
            worker.progress_updated.connect(self.update_progress)
            worker.finished.connect(self.download_finished)
            worker.log_message.connect(self.log_activity)
            
            def cleanup_thread():
                thread.quit()
                thread.wait()
                self.log_activity(f"Retry thread cleanup completed for {server_info}")
            
            worker.finished.connect(cleanup_thread)
            
            worker.moveToThread(thread)
            worker.set_thread(thread)
            thread.started.connect(worker.run)
            
            self.download_workers[server_info] = worker
            self.download_threads[server_info] = thread
            
            # Show progress bar
            if server_widget.progress_bar:
                server_widget.progress_bar.setVisible(True)
                server_widget.progress_bar.setValue(0)
            if server_widget.status_label:
                server_widget.status_label.setText("Retrying failed files...")
            
            thread.start()
            
            self.log_activity(
                f"Started retry for {total_failed} failed files across {len(stations_to_retry)} stations on {server_info}"
            )
            
        except json.JSONDecodeError:
            QMessageBox.critical(
                self,
                "Corrupted History",
                "Download history file is corrupted.\n\n"
                "Please clear the history using the History tab."
            )
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to prepare retry:\n{str(e)}")
            self.log_activity(f"Retry preparation failed: {str(e)}")

    def start_retry_download(self, server, stations_to_retry, local_folder, server_widget):
        """Start download worker for retrying failed files"""
        username = server['username']
        
        # Clean up any old workers
        if username in self.download_workers:
            old_worker = self.download_workers[username]
            old_thread = self.download_threads.get(username)
            
            try:
                old_worker.stop()
                if old_thread and old_thread.isRunning():
                    old_thread.quit()
                    old_thread.wait(2000)
                    if old_thread.isRunning():
                        old_thread.terminate()
                        old_thread.wait()
            except Exception as e:
                self.log_activity(f"Error cleaning up old thread: {e}")
            
            del self.download_workers[username]
            if username in self.download_threads:
                del self.download_threads[username]
        
        # Get all station IDs to retry
        station_ids = list(stations_to_retry.keys())
        
        # Use current date/time range from widget (or default to today)
        start_date = server_widget.start_date.date().toPyDate()
        end_date = server_widget.end_date.date().toPyDate()
        start_time = server_widget.start_time.time().toPyTime()
        end_time = server_widget.end_time.time().toPyTime()
        
        params = {
            'start_dt': datetime.combine(start_date, start_time),
            'end_dt': datetime.combine(end_date, end_time),
            'local_folder': local_folder,
            'retry_mode': True,  # Flag to indicate retry mode
            'retry_files': stations_to_retry  # Dict of {station_id: [filenames]}
        }
        
        # Create retry worker
        worker = RetryDownloadWorker(server, station_ids, params, self.db_manager, stations_to_retry)
        thread = QThread()
        
        worker.progress_updated.connect(self.update_progress)
        worker.finished.connect(self.download_finished)
        worker.log_message.connect(self.log_activity)
        
        def cleanup_thread():
            """Clean up thread after worker finishes"""
            thread.quit()
            thread.wait()
            self.log_activity(f"Retry thread cleanup completed for {username}")
        
        worker.finished.connect(cleanup_thread)
        
        worker.moveToThread(thread)
        worker.set_thread(thread)
        thread.started.connect(worker.run)
        
        self.download_workers[username] = worker
        self.download_threads[username] = thread
        
        # Show progress UI
        # if server_widget.progress_bar:
        #     server_widget.progress_bar.setVisible(True)
        #     server_widget.progress_bar.setValue(0)
        if server_widget.status_label:
            server_widget.status_label.setText("Retrying failed files...")
        
        thread.start()
        
        QMessageBox.information(
            self,
            "Retry Started",
            f"Retry download started for {len(station_ids)} station(s).\n\n"
            f"Total files to retry: {sum(len(files) for files in stations_to_retry.values())}"
        )

    def export_failed_files(self, server_info):
        """Export failed files list to CSV"""
        try:
            log_file = "download_log.json"
            
            if not os.path.exists(log_file):
                QMessageBox.warning(self, "No Data", "No download history found.")
                return
            
            with open(log_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Filter failed files
            failed_files = [
                entry for entry in data 
                if entry.get("username") == server_info and entry.get("status", "").lower() == "failed"
            ]
            
            if not failed_files:
                QMessageBox.information(self, "No Failures", f"No failed files found for {server_info}")
                return
            
            # Ask where to save
            filename, _ = QFileDialog.getSaveFileName(
                self,
                "Export Failed Files",
                f"failed_files_{server_info}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "CSV Files (*.csv)"
            )
            
            if filename:
                import csv
                with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['Timestamp', 'Username', 'Station ID', 'Filename', 'Error Message'])
                    
                    for entry in failed_files:
                        writer.writerow([
                            entry.get('timestamp', ''),
                            entry.get('username', ''),
                            entry.get('station_id', ''),
                            entry.get('filename', ''),
                            entry.get('message', '')
                        ])
                
                QMessageBox.information(
                    self,
                    "Export Successful",
                    f"Failed files list exported to:\n{filename}\n\nTotal: {len(failed_files)} files"
                )
                self.log_activity(f"Exported {len(failed_files)} failed files to {filename}")
                
        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"Failed to export:\n{str(e)}")

    
    def refresh_history(self):
        """Refresh download history display with smart filtering and limits."""
        try:
            log_file = "download_log.json"
            
            # Check if file exists
            if not os.path.exists(log_file):
                self.history_text.setPlainText("No download history yet.\n\nHistory will appear here after your first download.")
                self.history_stats_label.setText("Total: 0 | Success: 0 | Failed: 0")
                return

            # Read the log file with better error handling
            try:
                with open(log_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError as json_err:
                # JSON is corrupted - offer to fix it
                error_msg = f"⚠️ Download history file is corrupted!\n\n"
                error_msg += f"Error: {str(json_err)}\n\n"
                error_msg += "Would you like to:\n"
                error_msg += "1. Backup corrupted file and start fresh\n"
                error_msg += "2. Try to recover partial data\n\n"
                error_msg += "Click 'Yes' to backup and reset, 'No' to try recovery."
                
                reply = QMessageBox.question(
                    self,
                    "Corrupted History File",
                    error_msg,
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                )
                
                if reply == QMessageBox.StandardButton.Yes:
                    # Backup corrupted file
                    backup_name = f"download_log_corrupted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json.bak"
                    try:
                        shutil.copy(log_file, backup_name)
                        os.remove(log_file)
                        QMessageBox.information(
                            self,
                            "File Reset",
                            f"✅ Corrupted file backed up as:\n{backup_name}\n\nHistory has been reset."
                        )
                        self.history_text.setPlainText("History reset. New downloads will appear here.")
                        self.history_stats_label.setText("Total: 0 | Success: 0 | Failed: 0")
                    except Exception as e:
                        QMessageBox.critical(self, "Backup Failed", f"Could not backup file:\n{str(e)}")
                    return
                else:
                    # Try to recover partial data
                    data = self.attempt_json_recovery(log_file)
                    if not data:
                        self.history_text.setPlainText(
                            "❌ Could not recover data from corrupted file.\n\n"
                            "Please use 'Clear History' to reset, or manually delete:\n"
                            f"{os.path.abspath(log_file)}"
                        )
                        self.history_stats_label.setText("Error: Corrupted file")
                        return

            # Check if there's any data
            if not data or len(data) == 0:
                self.history_text.setPlainText("No download history yet.\n\nHistory will appear here after your first download.")
                self.history_stats_label.setText("Total: 0 | Success: 0 | Failed: 0")
                return

            # Get filter settings
            filter_limit = self.history_filter_combo.currentText()
            status_filter = self.status_filter_combo.currentText()
            
            # Apply status filter
            filtered_data = data
            if status_filter == "Success Only":
                filtered_data = [e for e in data if e.get("status", "").lower() == "success"]
            elif status_filter == "Failed Only":
                filtered_data = [e for e in data if e.get("status", "").lower() == "failed"]
            
            # Calculate statistics
            total_count = len(data)
            success_count = len([e for e in data if e.get("status", "").lower() == "success"])
            failed_count = len([e for e in data if e.get("status", "").lower() == "failed"])
            
            # Update statistics
            self.history_stats_label.setText(
                f"Total: {total_count} | Success: {success_count} | Failed: {failed_count} | "
                f"Showing: {len(filtered_data)} entries"
            )
            
            # Apply limit (get last N entries, most recent first)
            if filter_limit == "Last 100":
                display_data = filtered_data[-100:]
            elif filter_limit == "Last 500":
                display_data = filtered_data[-500:]
            elif filter_limit == "Last 1000":
                display_data = filtered_data[-1000:]
            else:  # All
                display_data = filtered_data
            
            # Reverse to show most recent first
            display_data = list(reversed(display_data))
            
            # Build history lines
            lines = []
            for entry in display_data:
                ts = entry.get("timestamp", "N/A")
                username = entry.get("username", "N/A")
                station = entry.get("station_id", "N/A")
                filename = entry.get("filename", "N/A")
                status = entry.get("status", "N/A")
                msg = entry.get("message", "N/A")
                
                # Color code status
                status_str = status.upper()
                lines.append(f"[{ts}] {username} | {station} | {filename} | {status_str} | {msg}")

            # Get current scroll position
            scroll_bar = self.history_text.verticalScrollBar()
            old_pos = scroll_bar.value() if scroll_bar else 0

            # Update text
            self.history_text.blockSignals(True)
            self.history_text.setPlainText("\n".join(lines))
            self.history_text.blockSignals(False)

            # Restore scroll position (only if not at bottom)
            if scroll_bar and old_pos < scroll_bar.maximum() - 50:
                scroll_bar.setValue(old_pos)
            else:
                # Auto-scroll to top for new entries
                scroll_bar.setValue(0)

        except Exception as e:
            error_text = f"❌ Error loading history: {str(e)}\n\n"
            error_text += "If this problem persists:\n"
            error_text += "1. Click 'Clear History' to reset\n"
            error_text += "2. Or manually delete: download_log.json"
            self.history_text.setPlainText(error_text)
            self.history_stats_label.setText("Error loading history")
            print(f"[ERROR] History refresh failed: {e}")
    
    def attempt_json_recovery(self, log_file):
        """Try to recover data from corrupted JSON file"""
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                content = f.read()
            
            # Try to find valid JSON objects
            recovered_data = []
            
            # Split by newlines and try to parse each line
            lines = content.split('\n')
            for line in lines:
                line = line.strip()
                if line.startswith('{') and line.endswith('}'):
                    try:
                        obj = json.loads(line)
                        if isinstance(obj, dict):
                            recovered_data.append(obj)
                    except:
                        continue
            
            if recovered_data:
                QMessageBox.information(
                    self,
                    "Partial Recovery",
                    f"✅ Recovered {len(recovered_data)} entries from corrupted file.\n\n"
                    "Some data may be lost. Consider exporting the recovered data."
                )
                return recovered_data
            
            return None
        except Exception as e:
            print(f"[ERROR] Recovery failed: {e}")
            return None
    
    def clear_history(self):
        """Clear history with confirmation"""
        reply = QMessageBox.question(
            self, 
            "Confirm Clear History", 
            "Are you sure you want to clear all download history?\n\nThis action cannot be undone.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            if os.path.exists("download_log.json"):
                os.remove("download_log.json")
            self.history_text.setPlainText("History cleared.")
            self.history_stats_label.setText("Total: 0 | Success: 0 | Failed: 0")
            self.log_activity("History cleared by user")
    
    def export_history(self):
        """Export history to file"""
        filename, _ = QFileDialog.getSaveFileName(self, "Export History", "download_history.txt", "Text Files (*.txt)")
        
        if filename:
            try:
                with open(filename, 'w') as f:
                    f.write(self.history_text.toPlainText())
                QMessageBox.information(self, "Success", f"History exported to {filename}")
                self.log_activity(f"History exported to {filename}")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to export history:\n{str(e)}")
    
    def log_activity(self, message):
        """Log activity to JSON file with error handling"""
        try:
            log_file = "activity_log.json"
            
            # Sanitize message to prevent JSON issues
            safe_message = str(message).replace('"', "'").replace('\n', ' ').replace('\r', ' ')
            
            log_entry = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "message": safe_message
            }
            
            data = []
            
            # Try to read existing log file
            if os.path.exists(log_file):
                try:
                    with open(log_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except json.JSONDecodeError as e:
                    # If file is corrupted, backup and start fresh
                    print(f"[WARN] Corrupted log file detected, creating backup...")
                    backup_file = f"activity_log_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    try:
                        os.rename(log_file, backup_file)
                        print(f"[INFO] Backup saved as: {backup_file}")
                    except:
                        pass
                    data = []
            
            # Append new entry
            data.append(log_entry)
            
            # Keep only last 10,000 entries to prevent file from growing too large
            if len(data) > 10000:
                data = data[-10000:]
            
            # Write to file with proper error handling
            temp_file = log_file + ".tmp"
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            # Replace old file with new one (atomic operation)
            if os.path.exists(log_file):
                os.remove(log_file)
            os.rename(temp_file, log_file)
            
        except Exception as e:
            # Don't crash the app if logging fails
            print(f"[ERROR] Activity log failed: {e}")
            # Try to print to console at least
            try:
                print(f"[LOG] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")
            except:
                pass

    def load_data(self):
        """Load all data"""
        if not self.db_manager:
            return
            
        self.refresh_servers_table()
        self.refresh_server_selection_lists()
        self.refresh_main_tabs()
        self.load_station_server_combo()
        self.refresh_history()
    
    def refresh_all_data(self):
        """Refresh all data after changes"""
        if not self.db_manager:
            return
            
        self.refresh_servers_table()
        self.refresh_server_selection_lists()
        self.refresh_main_tabs()
        self.load_station_server_combo()
    
    def load_station_server_combo(self):
        """Load servers into station settings combo"""
        if not self.db_manager:
            return
            
        servers = self.db_manager.get_servers() if self.db_manager else []
        self.station_server_combo.clear()
        
        for server in servers:
            display_text = server["username"]
            self.station_server_combo.addItem(display_text)
    
    def closeEvent(self, event):
        """Ensure all threads and resources are closed before exiting."""
        try:
            # Stop all download workers
            for username, worker in list(self.download_workers.items()):
                try:
                    worker.stop()
                    self.log_activity(f"Stopping worker for {username}")
                except Exception as e:
                    print(f"[WARN] Error stopping worker {username}: {e}")
            
            # Stop all threads
            for username, thread in list(self.download_threads.items()):
                try:
                    if thread.isRunning():
                        thread.quit()
                        thread.wait(3000)
                        if thread.isRunning():
                            thread.terminate()
                            thread.wait()
                        self.log_activity(f"Thread stopped for {username}")
                except Exception as e:
                    print(f"[WARN] Error stopping thread {username}: {e}")
            
            # Close database
            if hasattr(self, "db_manager") and self.db_manager:
                self.db_manager.close()
                print("[INFO] Database connection closed.")
            
            print("[INFO] All workers and threads stopped cleanly.")
        except Exception as e:
            print(f"[WARN] Error during window close: {e}")
        finally:
            super().closeEvent(event)


def main():
    """Main entry point with signal handling"""
    import signal
    
    app = QApplication(sys.argv)
    app.setApplicationName("FTP Downloader")
    app.setApplicationVersion("2.0")
    app.setOrganizationName("FTP Tools")

    try:
        app.setWindowIcon(QIcon("icon.ico"))
    except Exception as e:
        print(f"[WARN] Could not load icon: {e}")

    db = DatabaseManager()
    window = FTPDownloaderGUI(db)
    
    # Handle Ctrl+C gracefully
    def signal_handler(signum, frame):
        print("\n[INFO] Received interrupt signal, shutting down gracefully...")
        window.close()
        app.quit()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Allow Ctrl+C to work in PyQt
    timer = QTimer()
    timer.timeout.connect(lambda: None)
    timer.start(500)
    
    window.show()

    exit_code = 0
    try:
        exit_code = app.exec()
    except KeyboardInterrupt:
        print("[INFO] Application interrupted by user.")
    except Exception as e:
        print(f"[ERROR] Application error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        try:
            for username, worker in list(window.download_workers.items()):
                try:
                    worker.stop()
                    print(f"[INFO] Worker {username} stopped.")
                except Exception as e:
                    print(f"[WARN] Error stopping worker {username}: {e}")
            
            for username, thread in list(window.download_threads.items()):
                try:
                    if thread.isRunning():
                        thread.quit()
                        thread.wait(2000)
                        if thread.isRunning():
                            thread.terminate()
                except Exception as e:
                    print(f"[WARN] Error stopping thread {username}: {e}")
            
            db.close()
            print("[INFO] Database connection closed.")
        except Exception as e:
            print(f"[WARN] Cleanup error: {e}")

    sys.exit(exit_code)

def exception_hook(exctype, value, tb):
    """Handle uncaught exceptions"""
    import traceback
    print(f"\n[CRITICAL ERROR] Uncaught exception:")
    print(''.join(traceback.format_exception(exctype, value, tb)))
    
    # Log to file
    try:
        with open('crash_log.txt', 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"Crash at {datetime.now()}\n")
            f.write(''.join(traceback.format_exception(exctype, value, tb)))
    except:
        pass
    
    # Show error dialog
    try:
        QMessageBox.critical(
            None, 
            "Critical Error",
            f"An unexpected error occurred:\n\n{str(value)}\n\nThe application will now close.\n\nError details saved to crash_log.txt"
        )
    except:
        pass

sys.excepthook = exception_hook


if __name__ == "__main__":
    main()