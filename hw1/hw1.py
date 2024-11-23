import csv
import pyodbc
from pymongo import database


class DatabaseManager:
    def __init__(self, driver: str, server: str, username: str, password: str):
        self.connection_string = f"DRIVER={driver};SERVER={server};UID={username};PWD={password};DATABASE={username}"


    def file_to_database(self, path: str) -> None:
        try:
            connection = pyodbc.connect(self.connection_string)
            print("Connection established successfully")

            cursor = connection.cursor()
            with open(path, "r") as file:
                csv_file = csv.reader(file)

                for row in csv_file:
                    # validate both entries
                    if len(row) < 2:
                        continue
                    title = row[0]
                    prod_year = row[1]

                    # check that the values are in correct format
                    if not title or not prod_year.isdigit():
                        continue

                    title_length = len(title)  # Calculate the length of the title

                    # check that there aren't any duplicates
                    cursor.execute("SELECT COUNT(*) FROM dbo.MediaItems WHERE TITLE = ? AND PROD_YEAR = ?",
                                   (title, prod_year))
                    if cursor.fetchone()[0] > 0:
                        continue

                    cursor.execute(
                        "INSERT INTO dbo.MediaItems (TITLE, PROD_YEAR, TITLE_LENGTH) VALUES (?, ?, ?)",
                        (title, prod_year, title_length)
                    )
                    print("New entries were successfully inserted")
                    connection.commit()
        except Exception as e:
            print("Error", e)
        finally:
            if connection:
                connection.close()

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
    dm = DatabaseManager("ODBC Driver 18 for SQL Server", "132.72.64.124", "amirrot", "RPzc9yQh", "amirrot")
    # dm.file_to_database("C:\\Users\\Rotem\\Documents\\GitHub\\DBM\\hw1\\films.csv")
    # dm.calculate_similarity()
    dm.add_summary_items()