import csv
from multiprocessing.managers import Value

import pymongo
import bcrypt
import ast
import pandas as pd
import random
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import false


class LoginManager:

    def __init__(self) -> None:
        # MongoDB connection
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client["hw3"]
        self.collection = self.db["users"]
        self.salt = b"$2b$12$ezgTynDsK3pzF8SStLuAPO"  # TODO: if not working, generate a new salt

    def register_user(self, username: str, password: str) -> None:
        if not username or not password:
            raise ValueError("Username and password are required.")

        if len(username) < 3 or len(password) < 3:
            raise ValueError("Username and password must be at least 3 characters.")

        if self.collection.find_one({"username": username}):
            raise ValueError(f"User already exists: {username}.")

        hashed_pass = bcrypt.hashpw(password.encode('utf-8'), self.salt)
        self.collection.insert_one({"username": username, "password": hashed_pass})

    def login_user(self, username: str, password: str) -> object:
        hashed_pass = bcrypt.hashpw(password.encode('utf-8'), self.salt)
        user = self.collection.find_one({"username": username, "password": hashed_pass})
        if user is not None:
            print(f"Logged in successfully as: {username}")
        else:
            raise ValueError("Invalid username or password")
        pass


class DBManager:

    def __init__(self) -> None:
        # MongoDB connection
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client["hw3"]
        self.user_collection = self.db["users"]
        self.game_collection = self.db["games"]

    def load_csv(self) -> None:
        csv_file = 'NintendoGames.csv'
        data = pd.read_csv(csv_file)

        data['user_score'] = data['user_score'].astype(float)
        data['genres'] = data['genres'].apply(ast.literal_eval)
        data['is_rented'] = False
        # make sure that there are no duplicates
        games_to_add = []
        for _, row in data.iterrows():
            game_data = row.to_dict()
            if not self.game_collection.find_one({'title': game_data['title']}):
                games_to_add.append(game_data)
        # insert the remaining records
        if games_to_add:
            self.game_collection.insert_many(games_to_add)

    def rent_game(self, user: dict, game_title: str) -> str:
        game = self.game_collection.find_one({'title': game_title})

        if not game:
            return f"{game_title} not found"

        if game.get('is_rented', False):
            return f"{game_title} is already rented"

        self.game_collection.update_one(
            {'_id': game['_id']},
            {'$set': {'is_rented': True}}
        )

        self.user_collection.update_one(
            {'_id': user['_id']},
            {'$push': {'rented_games_ids': game['_id']}}
        )

        return f"{game_title} rented successfully"

    def return_game(self, user: dict, game_title: str) -> str:
        game = self.game_collection.find_one({'title': game_title})

        if not game:
            return "failure"

        if game.get("is_rented", False) and game["_id"] in user.get("rented_games_ids", []):
            self.game_collection.update_one({"_id": game["_id"]}, {"$set": {"is_rented": False}})
            self.user_collection.update_one(
                {"_id": user["_id"]},
                {"$pull": {"rented_games_ids": game["_id"]}}
            )
            return f"{game_title} returned successfully"

        return f"{game_title} was not rented by you"

    def recommend_games_by_genre(self, user: dict) -> list:
        rented_game_ids = user.get('rented_games_ids', [])
        if not rented_game_ids:
            return ["No games rented"]
        # select random genre from games rented
        pipeline_genres = [
            {'$match': {'_id': {'$in': rented_game_ids}}},
            {'$unwind': '$genres'},
            {'$group': {'_id': '$genres', 'count': {'$sum': 1}}}
        ]
        genres_data = list(self.game_collection.aggregate(pipeline_genres))
        genres = [g['_id'] for g in genres_data]
        weights = [g['count'] for g in genres_data]
        selected_genre = random.choices(genres, weights=weights, k=1)[0]

        # select 5 random games
        pipeline_games = [
            {'$match': {'genres': selected_genre}},
            {'$sample': {'size': 5}}
        ]
        random_games = list(self.game_collection.aggregate(pipeline_games))
        game_titles = [game['title'] for game in random_games]

        return game_titles


    def recommend_games_by_name(self, user: dict) -> list:
        rented_game_ids = user.get("rented_games_ids", [])

        if not rented_game_ids:
            return ["No games rented"]

        rented_games = list(self.game_collection.find({"_id": {"$in": rented_game_ids}}))

        if not rented_games:
            return ["No games rented"]

        rand_game = random.choice(rented_games)
        game_title = rand_game["title"]

        all_games = list(self.game_collection.find())
        all_games_title = [game["title"] for game in all_games if game["_id"] not in rented_game_ids] #TODO -> make sure that all game titles are from the games that are not rented

        vectorizer = TfidfVectorizer()
        tfidf_mat = vectorizer.fit_transform([game_title] + all_games_title)

        similarities = cosine_similarity(tfidf_mat[0:1], tfidf_mat[1:]).flatten()

        similarities = similarities.argsort()[::-1]
        recommended_titles = [all_games_title[i] for i in similarities[:5]]

        return recommended_titles

    def find_top_rated_games(self, min_score) -> list:
        return list(
            self.game_collection.find(
                {'user_score': {'$gte': min_score}},
                {'title': 1, 'user_score': 1, '_id': 0}
            )
        )

    def decrement_scores(self, platform_name) -> None:
        self.game_collection.update_many(
            {'platform': platform_name},
            {'$inc': {'user_score': -1}}
        )

    def get_average_score_per_platform(self) -> dict:
        pipeline = [
            {"$group": {
                "_id": "$platform",
                "average_score": {"$avg": "$user_score"}
            }},
            {"$project": {
                "_id": 1,
                "average_score": {"$round": ["$average_score", 3]}
            }}
        ]

        results = self.game_collection.aggregate(pipeline)
        return {result["_id"]: result["average_score"] for result in results}


    def get_genres_distribution(self) -> dict:
        pipeline_genres = [
            {'$unwind': '$genres'},
            {'$group': {'_id': '$genres', 'count': {'$sum': 1}}}
        ]
        genres_data = list(self.game_collection.aggregate(pipeline_genres))
        return {item['_id']: item['count'] for item in genres_data}


if __name__ == '__main__':
    db_manager = DBManager()
    db_manager.load_csv()
    login_manager = LoginManager()

    # # Step 1: Test user registration
    # print("\n=== Testing User Registration ===")
    # try:
    #     print("Registering user1...")
    #     login_manager.register_user("user1", "password1")
    #     print("Registering user2...")
    #     login_manager.register_user("user2", "password2")
    #     print("Attempting to register an existing user...")
    #     login_manager.register_user("user1", "password1")  # Should raise an error
    # except ValueError as e:
    #     print(f"Error: {e}")

    # Step 2: Test user login
    # print("\n=== Testing User Login ===")
    # try:
    #     print("Logging in as user1...")
    #     login_manager.login_user("user1", "password1")
    #     print("Attempting to login with wrong password...")
    #     login_manager.login_user("user1", "wrongpassword")  # Should raise an error
    # except ValueError as e:
    #     print(f"Error: {e}")

    # # Step 3: Test loading games from the database
    # print("\n=== Testing Loaded Games ===")
    # print(f"Total games in collection: {db_manager.game_collection.count_documents({})}")

    # Step 4: Test renting a game
    # print("\n=== Testing Game Renting ===")
    # user1 = db_manager.user_collection.find_one({"username": "user1"})
    # try:
    #     # print("Renting a game...")
    #     # result = db_manager.rent_game(user1, "Pikmin 4")
    #     # print(result)
    #     print("Renting the same game again (should fail)...")
    #     result = db_manager.rent_game(user1, "Pikmin 4")  # Should fail
    #     print(result)
    # except ValueError as e:
    #     print(f"Error: {e}")
    #
    # # Step 5: Test returning a game
    # print("\n=== Testing Game Returning ===")
    # try:
    #     print("Returning a rented game...")
    #     result = db_manager.return_game(user1, "Pikmin 4")
    #     print(result)
    #     print("Returning a game not rented by the user (should fail)...")
    #     result = db_manager.return_game(user1, "Pikmin 4")
    #     print(result)
    # except ValueError as e:
    #     print(f"Error: {e}")
    #
    # # Step 6: Test recommending games by genre
    # print("\n=== Testing Game Recommendation by Genre ===")
    # recommendations = db_manager.recommend_games_by_genre(user1)
    # print("Recommended games by genre:", recommendations)
    #
    # # Step 7: Test recommending games by name
    # print("\n=== Testing Game Recommendation by Name ===")
    # recommendations = db_manager.recommend_games_by_name(user1)
    # print("Recommended games by name:", recommendations)

    # # Step 8: Test finding top-rated games
    # print("\n=== Testing Top-Rated Games ===")
    # top_rated = db_manager.find_top_rated_games(8.5)
    # print("Top-rated games with score >= 8.5:", top_rated)

    # Step 9: Test decrementing scores
    # print("\n=== Testing Score Decrement ===")
    # print("Decrementing scores for platform 'Switch'...")
    # db_manager.decrement_scores("Switch")
    # print("Scores decremented.")

    # Step 10: Test average score per platform
    print("\n=== Testing Average Score Per Platform ===")
    avg_scores = db_manager.get_average_score_per_platform()
    print("Average scores per platform:", avg_scores)

    # # Step 11: Test genre distribution
    # print("\n=== Testing Genre Distribution ===")
    # genre_distribution = db_manager.get_genres_distribution()
    # print("Genre distribution:", genre_distribution)
