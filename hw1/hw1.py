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
        try:
            connection = pyodbc.connect(self.connection_string)
            print("Connection established successfully")

            cursor = connection.cursor()

            cursor.execute("SELECT MID FROM dbo.MediaItems")
            all_MIDs = [row[0] for row in cursor.fetchall()]

            cursor.execute("SELECT dbo.MaximalDistance()")
            maximal_distance = cursor.fetchone()[0]

            # calculate the similarity
            for i in range(len(all_MIDs)):
                for j in range(i + 1, len(all_MIDs)):  # Ensure unique pairs (MID1, MID2)
                    mid1 = all_MIDs[i]
                    mid2 = all_MIDs[j]

                    # check that mids are different
                    if mid1 == mid2:
                        continue
                    # Call the SimCalculation function to get the similarity between the two MIDs
                    cursor.execute("SELECT dbo.SimCalculation(?, ?, ?)", mid1, mid2, maximal_distance)
                    similarity = cursor.fetchone()[0]

                    # check that it's not a duplicate
                    cursor.execute("SELECT * FROM dbo.Similarity WHERE MID1 = ? AND MID2 = ?",
                                   (mid2, mid1))
                    if cursor.fetchone() is not None:
                        if cursor.fetchone()[0] > 0:
                            continue

                    # Insert or update the Similarity table
                    cursor.execute("""
                                IF EXISTS (SELECT 1 FROM dbo.Similarity WHERE MID1 = ? AND MID2 = ?)
                                BEGIN
                                    UPDATE dbo.Similarity
                                    SET SIMILARITY = ?
                                    WHERE MID1 = ? AND MID2 = ?
                                END
                                ELSE
                                BEGIN
                                    INSERT INTO dbo.Similarity (MID1, MID2, SIMILARITY)
                                    VALUES (?, ?, ?)
                                END
                            """, mid1, mid2, similarity, mid1, mid2, mid1, mid2, similarity)
                    print(f"Similarity was successfully inserted MID1:{mid1}, MID2{mid2}")

            # commit to database
            connection.commit()
            print(f"similarity between {mid1} and {mid2} was successfully inserted")
        except Exception as e:
            print("Error", e)
        finally:
            if connection:
                connection.close()

    def print_similar_items(self, mid: int) -> None:
        try:
            connection = pyodbc.connect(self.connection_string)
            print("Connection established successfully")

            cursor = connection.cursor()
            # retrieve items with similarity >= 0.25
            cursor.execute(
                """SELECT s.MID1, s.MID2, s.SIMILARITY, m.TITLE
                      FROM dbo.Similarity s
                      INNER JOIN dbo.MediaItems m
                      ON (s.MID1 = m.MID AND s.MID2 = ?) OR (s.MID2 = m.MID AND s.MID1 = ?)
                      WHERE s.SIMILARITY >= 0.25
                      ORDER BY s.SIMILARITY ASC;
                  """, mid, mid)

            results = cursor.fetchall()
            if not results:
                print(f"No similar items found for MID {mid}")
                return

            for row in results:
                mid1, mid2, similarity, title = row
                print(f"{title} {similarity:.2f}")

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            if connection:
                connection.close()

    def add_summary_items(self) -> None:
        try:
            connection = pyodbc.connect(self.connection_string)
            cursor = connection.cursor()
            # Execute the stored procedure
            cursor.execute("EXEC AddSummaryItems")
            connection.commit()  # Commit changes to the database
            print("Summary items added successfully.")

        except Exception as e:
            print(f"An error occurred while executing AddSummaryItems: {e}")

        finally:
            if connection:
                connection.close()

if __name__ == '__main__':
    dm = DatabaseManager("ODBC Driver 18 for SQL Server", "132.72.64.124", "amirrot", "RPzc9yQh", "amirrot")
    # dm.file_to_database("C:\\Users\\Rotem\\Documents\\GitHub\\DBM\\hw1\\films.csv")
    # dm.calculate_similarity()
    dm.add_summary_items()