import sqlite3


conn = sqlite3.connect('cheaters.db')
cursor = conn.cursor()
cursor.execute('''CREATE TABLE cheaters_data (
                    timestamp TEXT,
                    player_id INTEGER,
                    event_id INTEGER,
                    error_id INTEGER,
                    json_server TEXT,
                    json_client TEXT
                )''')
conn.commit()
conn.close()