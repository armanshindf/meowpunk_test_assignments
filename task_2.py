import sqlite3
import csv
from datetime import datetime, timedelta
from memory_profiler import profile


class DataProcessor:


    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.conn.close()
    
    @profile
    def load_data(self, client_file, server_file, date):
        with open(client_file, 'r') as f:
            client_data = list(csv.reader(f))
        with open(server_file, 'r') as f:
            server_data = list(csv.reader(f))

# Фильтруем данные по дате
        client_data = [row for row in client_data if row[0].startswith(date)]
        server_data = [row for row in server_data if row[0].startswith(date)]

# Соединяем данные по error_id
        joined_data = []
        for client_row in client_data:
            for server_row in server_data:
                if client_row[2] == server_row[2]:
                    joined_data.append([server_row[0], client_row[1], server_row[1], server_row[2], server_row[3], client_row[3]])

# Исключаем записи с player_id из таблицы cheaters
        banned_players = self.get_banned_players(date)
        filtered_data = []
        for row in joined_data:
            if row[1] not in banned_players:
                filtered_data.append(row)

# Выгружаем данные в таблицу cheaters_data
        self.cursor.executemany("INSERT INTO cheaters_data VALUES (?, ?, ?, ?, ?, ?)", filtered_data)
        self.conn.commit()

    @profile
    def get_banned_players(self, date):
        banned_players = []
        ban_time = datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)
        for row in self.cursor.execute("SELECT player_id FROM cheaters WHERE ban_time <= ?", (ban_time,)):
            banned_players.append(row[0])
        return banned_players


processor = DataProcessor('cheaters.db')
processor.load_data('client.csv', 'server.csv', '2023-08-29')