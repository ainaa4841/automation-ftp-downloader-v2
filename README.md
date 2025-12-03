# FTP Downloader v2.0

A robust, multi-threaded FTP file downloader with a user-friendly GUI built with PyQt6. Designed for automated and scheduled downloading of time-series data files from multiple FTP servers.

![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey)

## 📋 Features

- **Multi-Server Management**: Configure and manage multiple FTP servers with different credentials
- **Station-Based Downloads**: Organize downloads by station IDs with flexible selection
- **Smart Date/Time Filtering**: Download files within specific date and time ranges
- **Intelligent File Detection**: Automatically parses filenames to extract station IDs and timestamps
- **Multi-Threaded Downloads**: Configurable concurrent downloads with automatic thread scaling
- **Progress Tracking**: Real-time progress bars and status updates
- **Download History**: Complete logging of all downloads (success/failed) with export functionality
- **Retry Failed Downloads**: One-click retry of failed downloads with detailed failure reports
- **Auto-Download Scheduling**: Schedule automatic downloads at specific times
- **Duplicate Detection**: Automatically skips files that already exist locally
- **Connection Management**: Automatic reconnection and error recovery
- **PostgreSQL Database**: Persistent storage of server configurations and station data

## 🖼️ Screenshots

### Main Interface
The main download interface with server tabs, station selection, and progress tracking.

### Settings
Configure FTP servers, manage stations, and select active servers.

### History
View detailed download history with filtering and export options.

## 🚀 Installation

### Prerequisites

- Python 3.8 or higher
- PostgreSQL 12 or higher

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/ftp-downloader.git
cd ftp-downloader
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Set Up PostgreSQL Database

Create a PostgreSQL database and user:

```sql
CREATE DATABASE your_database;
CREATE USER your_username WITH PASSWORD '123456';
GRANT ALL PRIVILEGES ON DATABASE your_database TO your_username;
```

### 4. Configure Database Connection

Edit `db_config.json` with your database credentials:

```json
{
    "host": "localhost",
    "port": 5432,
    "database": "your_database",
    "username": "your_username",
    "password": "123456"
}
```

### 5. Run the Application

```bash
python main.py
```

## 📦 Dependencies

```
PyQt6>=6.4.0
psycopg2-binary>=2.9.5
```

## 📖 Usage Guide

### Setting Up Servers

1. Go to **Settings > Server Settings**
2. Enter FTP server details:
   - IP Address
   - Port (default: 21)
   - Username
   - Password
   - Remote Path (optional)
3. Click **Test Connection** to verify
4. Click **Add Server** to save

### Managing Stations

1. Go to **Settings > Station Settings**
2. Select a server from the dropdown
3. Enter station ID (e.g., `STATION1`, `STATION2`)
4. Click **Add Station**

### Selecting Servers for Download

1. Go to **Settings > Select Servers**
2. Select servers from the **Saved Servers** list
3. Click **Add →** to move them to **Selected Servers**
4. Only selected servers will appear in the Main tab

### Downloading Files

1. Go to **Main** tab
2. Select a server tab
3. Move stations from **Available Stations** to **Selected Stations** using **Add →** button
4. Configure settings:
   - **Local Folder**: Where files will be downloaded
   - **Date Range**: Start and end dates
   - **Time Range**: Filter files by time
5. Click **Start Download**

### Scheduling Auto-Downloads

1. Check **Enable Auto Download at**
2. Set the desired time
3. Click **Save** to schedule
4. The download will run automatically at the specified time

### Viewing History

1. Go to **History** tab
2. Use filters to view:
   - Last 100/500/1000 entries or All
   - Success Only / Failed Only / All
3. Export history using **Export History** button

### Retrying Failed Downloads

1. After a download completes with failures, a dialog will appear
2. Click **🔄 Retry Failed Files**
3. Or view failed files and export the list for analysis

## 📁 Project Structure

```
ftp-downloader/
├── main.py                 # Main application entry point
├── ftp_downloader.py       # FTP download logic and file management
├── database.py             # Database operations and connection management
├── db_config.json          # Database configuration
├── requirements.txt        # Python dependencies
├── README.md              # This file
├── download_log.json      # Download history (auto-generated)
├── activity_log.json      # Application activity log (auto-generated)
└── ftp_downloader.log     # Application logs (auto-generated)
```

## 🗂️ File Organization

Downloaded files are organized as follows:

```
LocalFolder/
└── STATION_ID/
    └── DDMMYYYY_DDMMYYYY/
        ├── file1.txt
        ├── file2.txt
        └── file3.txt
```

**Example:**
```
C:/Downloads/
└── STATION1/
    └── 18112025_20112025/
        ├── STATION1251118170000.txt
        ├── STATION1251119170000.txt
        └── STATION1251120170000.txt
```

## 🔧 Configuration

### Threading Configuration

The application automatically adjusts thread count based on file count:

| Files | Threads | Batch Update Interval |
|-------|---------|----------------------|
| > 10,000 | 3 | 100 |
| > 5,000 | 5 | 50 |
| > 2,000 | 6 | 30 |
| > 1,000 | 8 | 20 |
| > 500 | 10 | 15 |
| > 100 | 12 | 10 |
| ≤ 100 | 8 | 5 |

### Filename Pattern Support

The application supports the following filename patterns:

1. **Standard format**: `STATIONID` + `YYMMDDHHMMSS` + `.txt`
   - Example: `STATION1251118104500.txt`
   - Station: `STATION1`
   - Date/Time: `2025-11-18 10:45:00`

2. **Extended format**: `STATIONID` + `YYMMDDHHMMSS` + `_` + timestamp + `.txt`
   - Example: `STATION1RF251108170000_20251108170535.txt`
   - Station: `STATION1RF`
   - Date/Time: `2025-11-08 17:00:00`

### Remote Path Detection

The application automatically searches multiple common FTP directory structures:

- `/ARCHIVE/YYYY/MM/DD/`
- `/received/YYYY/MM/DDMMYYYY/`
- `/archived/YYYY/MM/DDMMYYYY/`
- `/YYYY/MM/DD/`
- `/data/YYYY/MM/DD/`
- `/STATION_ID/YYYY/MM/DD/`
- And more...

## 🛠️ Troubleshooting

### Database Connection Issues

**Problem**: "Failed to connect to the database"

**Solution**:
1. Ensure PostgreSQL is running
2. Verify database credentials in `db_config.json`
3. Check if the database `ftp_db` exists
4. Ensure user has proper permissions

### FTP Connection Timeout

**Problem**: Downloads fail with timeout errors

**Solution**:
1. Reduce thread count by manually editing `ftp_downloader.py`
2. Check network connectivity
3. Verify FTP server allows multiple connections
4. Increase timeout in `ftp_connect()` function

### Corrupted Download History

**Problem**: "Download history file is corrupted"

**Solution**:
1. Go to **History** tab
2. Click **Clear History**
3. Or manually delete `download_log.json`

### Files Not Being Detected

**Problem**: Application can't find files on FTP server

**Solution**:
1. Use **Preview Directory** to see actual file structure
2. Check if filename format matches supported patterns
3. Verify date/time range includes the files
4. Check if station ID matches exactly (case-sensitive)

## 🔐 Security Notes

- Store database credentials securely
- Consider using environment variables for sensitive data
- FTP credentials are stored in PostgreSQL (use encrypted connections in production)
- The application uses plain FTP (not FTPS/SFTP) - consider security implications

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🐛 Known Issues

- Large download history files (> 50,000 entries) may cause UI lag
- Concurrent downloads from the same server may hit connection limits
- Windows file locking can occasionally cause log write delays

## 🗺️ Roadmap

- [ ] FTPS/SFTP support
- [ ] Email notifications for completed downloads
- [ ] Bandwidth throttling options
- [ ] Download queue management
- [ ] Multi-language support
- [ ] Dark mode theme
- [ ] Docker container support

## 📧 Contact

For questions, issues, or suggestions:
- Create an issue on GitHub
- Email: ainamhmdd@gmail.com

## 🙏 Acknowledgments

- Built with PyQt6 for cross-platform GUI
- PostgreSQL for reliable data storage
- Community contributors and testers

---

**Made with ❤️ by Aina Mahmod**
