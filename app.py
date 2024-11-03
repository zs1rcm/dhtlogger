import libtorrent as lt
import time
import sqlite3
from datetime import datetime
import re

# Initialize SQLite database connection and create table if not exists
conn = sqlite3.connect('dht_metadata.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS torrents (
        torrent_hash TEXT PRIMARY KEY,
        torrent_name TEXT,
        files TEXT,
        magnet_link TEXT,
        firstseen TIMESTAMP,
        lastseen TIMESTAMP,
        status TEXT
    )
''')
conn.commit()

# Initialize session with DHT settings
session = lt.session({'listen_interfaces': '0.0.0.0:6881'})
session.apply_settings({
    'enable_dht': True,
    'enable_lsd': True,
    'enable_upnp': True,
    'enable_natpmp': True
})

# Adding DHT bootstrap nodes
dht_bootstrap_nodes = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
    ("router.bitcomet.com", 6881),
    ("dht.aelitis.com", 6881)
    ]
for node in dht_bootstrap_nodes:
    session.add_dht_router(node[0], node[1])

# Setting the alert mask to include all alert categories for debugging
session.set_alert_notify(lambda: None)
session.set_alert_mask(lt.alert.category_t.all_categories)

# Open file to write hash, metadata, and magnet link
log_file = open("dht_metadata_log.txt", "a")

def log_to_file(torrent_hash, torrent_name, files, magnet_link, status):
    # Write metadata or timeout to the log file
    log_file.write("="*50 + "\n")
    log_file.write(f"Torrent Hash: {torrent_hash}\n")
    log_file.write(f"Torrent Name: {torrent_name}\n" if torrent_name else "Torrent Name: Unknown\n")
    log_file.write("Files:\n" + (files if files else " - No files available\n"))
    log_file.write(f"Magnet Link: {magnet_link}\n")
    log_file.write(f"Status: {status}\n")
    log_file.write("="*50 + "\n")
    log_file.flush()  # Ensure it’s written immediately
    print(f"Logged to file: {torrent_hash} - {status}")

def insert_or_update_torrent(torrent_hash, torrent_name, files, magnet_link, status):
    # Convert current time to string format
    current_time = datetime.now()
    
    # Check if the torrent already exists in the database
    cursor.execute("SELECT torrent_hash FROM torrents WHERE torrent_hash = ?", (torrent_hash,))
    data = cursor.fetchone()
    
    if data is None:
        # Insert new torrent with firstseen and lastseen set to current time
        cursor.execute('''
            INSERT INTO torrents (torrent_hash, torrent_name, files, magnet_link, firstseen, lastseen, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (torrent_hash, torrent_name, files, magnet_link, current_time, current_time, status))
        print(f"New torrent added to database: {torrent_hash} - {status}")
    else:
        # Update lastseen and status for an existing torrent
        cursor.execute('''
            UPDATE torrents
            SET lastseen = ?, status = ?
            WHERE torrent_hash = ?
        ''', (current_time, status, torrent_hash))
        print(f"Updated torrent in database: {torrent_hash} - {status}")
    
    conn.commit()  # Save changes to the database

def extract_name_from_alert(alert):
    # Convert the alert message to a string
    message = alert.message()
    print(f"Full alert message: {message}")  # Debugging output to examine alert format

    # Use a broader regex pattern to try to capture the name field more accurately
    match = re.search(r"'name':\s?'([^']*)'", message)
    if match:
        return match.group(1)
    return "Unknown"


def write_metadata(info_hash, torrent_name="Unknown"):
    # Check if the torrent hash has been previously downloaded successfully
    cursor.execute("SELECT status FROM torrents WHERE torrent_hash = ?", (info_hash,))
    result = cursor.fetchone()
    
    if result and result[0] == "Success":
        print(f"Metadata already downloaded for {info_hash}. Skipping.")
        # Update lastseen timestamp without re-downloading
        insert_or_update_torrent(info_hash, torrent_name, None, None, "Success")
        return

    try:
        magnet_uri = f"magnet:?xt=urn:btih:{info_hash}"
        handle = lt.add_magnet_uri(session, magnet_uri, {"save_path": "./torrent_data/"})
        print(f"Fetching metadata for torrent: {info_hash}")

        # Wait for metadata download to complete, with a timeout
        timeout = 60  # seconds
        start_time = time.time()
        while not handle.has_metadata():
            if time.time() - start_time > timeout:
                print(f"Timeout: Could not retrieve metadata for {info_hash}")
                # Log the timeout to both file and database using the provided torrent name
                log_to_file(info_hash, torrent_name, None, magnet_uri, "Timed Out")
                insert_or_update_torrent(info_hash, torrent_name, None, magnet_uri, "Timed Out")
                return
            time.sleep(1)

        torrent_info = handle.get_torrent_info()

        # Collect metadata information
        torrent_name = torrent_info.name()
        files = "\n".join(f" - {file.path} ({file.size} bytes)" for file in torrent_info.files())

        # Log successful metadata retrieval to both file and database
        log_to_file(info_hash, torrent_name, files, magnet_uri, "Success")
        insert_or_update_torrent(info_hash, torrent_name, files, magnet_uri, "Success")

        # Remove torrent from session
        session.remove_torrent(handle)
    except Exception as e:
        print(f"Error fetching metadata for {info_hash}: {e}")
        # Log the error to both file and database with the provided torrent name
        log_to_file(info_hash, torrent_name, None, magnet_uri, f"Error: {e}")
        insert_or_update_torrent(info_hash, torrent_name, None, magnet_uri, "Error")

try:
    while True:
        # Collect DHT get_peers messages and print all alerts
        alerts = session.pop_alerts()
        for alert in alerts:
            if isinstance(alert, lt.dht_announce_alert):
                info_hash = str(alert.info_hash).replace("<sha1_hash ", "").replace(">", "")
                
                # Extract torrent name from alert using the helper function
                torrent_name = extract_name_from_alert(alert)
                print(f"Discovered info_hash: {info_hash} with name: {torrent_name}")
                
                # Pass the extracted torrent name to write_metadata
                write_metadata(info_hash, torrent_name)
            elif isinstance(alert, lt.dht_stats_alert):
                print(f"DHT nodes: {alert.num_nodes}, DHT active requests: {alert.num_peers}")
            elif isinstance(alert, lt.log_alert):
                print(f"Log: {alert.message()}")
            else:
                print(f"Alert received: {alert}")

        time.sleep(5)
except KeyboardInterrupt:
    print("Stopping DHT monitor.")
finally:
    log_file.close()  # Close the log file when the program ends
    conn.close()  # Close the database connection when the program ends
