import csv

import pyodbc
from pymongo import database


class DatabaseManager:
    def __init__(self, driver: str, server: str, username: str, password: str, database: str):
        connection_string = f"DRIVER={driver};SERVER={server};UID={username};PWD={password};DATABASE={database}"
        self.connection=pyodbc.connect(connection_string)
        print("Connection established successfully")

    def file_to_database(self, path: str) -> None:
        try:
            cursor = self.connection.cursor()
            with open(path, "r") as file:
                csv_file = csv.reader(file)

                for row in csv_file:
                    if len(row) < 2:
                        continue
                    title = row[0]
                    prod_year = row[1]
                    title_length = len(title)  # Calculate the length of the title

                    cursor.execute(
                        "INSERT INTO MediaItems (TITLE, PROD_YEAR, TITLE_LENGTH) VALUES (?, ?, ?)",
                        (title, prod_year, title_length)
                    )
                    self.connection.commit()
        except Exception as e:
            print("Error", e)
        finally:
            if self.connection:
                self.connection.close()

    def calculate_similarity(self) -> None:
        # TODO
        pass

    def print_similar_items(self, mid: int) -> None:
        # TODO
        pass
    def add_summary_items(self) -> None:
        # TODO
        pass    

if __name__ == '__main__':
    print("hello")