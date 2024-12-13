import csv

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime, LargeBinary, text
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime, timezone, timedelta
from sqlalchemy import func
import bcrypt

Base = declarative_base()




class User(Base):
    __tablename__ = "Users"

    id = Column(String(255), primary_key=True)
    password = Column(LargeBinary)
    first_name = Column(String(255))
    last_name = Column(String(255))
    date_of_birth = Column(DateTime)
    registration_date = Column(DateTime)
    histories = relationship("History", back_populates="user", cascade="all, delete-orphan")

    def __init__(
        self,
        username,
        password,
        first_name,
        last_name,
        date_of_birth,
        registration_date,
    ):
        self.id = username
        self.password =  bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        self.first_name = first_name
        self.last_name = last_name
        self.date_of_birth = date_of_birth
        self.registration_date = registration_date
        self.histories = []


    def add_history(self, media_item_id):
        new_history = History(self.id, media_item_id, datetime.now(timezone.utc))
        self.histories.insert(0, new_history)


    def sum_title_length (self):
        length = 0
        for history in self.histories:
            if history.mediaitem:
                length += history.mediaitem.title_length
        return length


class MediaItem(Base):
    __tablename__ = "MediaItems"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(255), nullable=False)
    prod_year = Column(Integer, nullable=False)
    title_length = Column(Integer, nullable=False)

    def __init__(self, title, prod_year, title_length):
        self.title = title
        self.prod_year = prod_year
        self.title_length = title_length

class History(Base):
    __tablename__ = "History"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(255), ForeignKey('Users.id'), nullable=False)
    user = relationship("User", back_populates="histories")
    #     user = relationship("User", back_populates="histories", cascade='all, delete-orphan')
    media_item_id = Column(Integer, ForeignKey('MediaItems.id'), nullable=False)
    mediaitem = relationship('MediaItem')
    #     mediaitem = relationship("MediaItem", back_populates="histories", cascade='all, delete', uselist=False)
    viewtime = Column(DateTime)

    def __init__(self, user_id, media_item_id, viewtime):
        self.user_id = user_id
        self.media_item_id = media_item_id
        self.viewtime = viewtime

class Repository:
    def __init__(self, model_class):
        self.model_class=model_class

    def get_by_id(self, session, entity_id):
        return session.query(self.model_class).filter(self.model_class.id == entity_id).first()
    
    def get_all(self,session):
        return session.query(self.model_class).all()
    
    def delete(self,session, entity):
        session.delete(entity)

    def add(self, session, entity):
        session.add(entity)


class UserRepository(Repository):
    def __init__(self):
        super().__init__(User)
   
    def validateUser(self,session, username: str, password: str) -> bool:
        user = session.query(self.model_class).filter(self.model_class.id == username).first()

        if not user:
            return False

        if bcrypt.checkpw(password.encode('utf-8'), user.password):
            return True

        return False

    def getNumberOfRegistredUsers(self,session, n: int) -> int:
        start_date = (datetime.now() - timedelta(days=n)).date()
        return session.query(User).filter(User.registration_date >= start_date).count()
    
class ItemRepository(Repository):
    def __init__(self):
        super().__init__(MediaItem)

    def getTopNItems(self, session, top_n: int) -> list:
        return session.query(MediaItem).order_by(MediaItem.id.asc()).limit(top_n).all()


    
class UserService:
    def __init__(self, session, user_repo: UserRepository):
        self.user_repo = user_repo
        self.session = session

    def create_user(self, username, password, first_name, last_name, date_of_birth):
        new_user = User(
            username,
            password,
            first_name,
            last_name,
            date_of_birth,
            registration_date=datetime.now()  # Set registration date to current time
        )
        self.user_repo.add(self.session, new_user)
        self.session.commit()

    def add_history_to_user(self, username, media_item_id):
        user = self.user_repo.get_by_id(self.session, username)
        if not user:
            raise ValueError("User not found")
        user.add_history(media_item_id)
        self.session.commit()
    
    def validateUser(self, username: str, password: str) -> bool:
        return self.user_repo.validateUser(self.session, username, password)

    def getNumberOfRegistredUsers(self, n: int) -> int:
        return  self.user_repo.getNumberOfRegistredUsers(self.session, n)
    
    def sum_title_length_to_user(self, username):
        user = self.user_repo.get_by_id(self.session,  username)
        return user.sum_title_length()

    def get_all_users(self):
        return self.user_repo.get_all(self.session)

    
class ItemService:
    def __init__(self, session, item_repo:ItemRepository):
        self.item_repo=item_repo
        self.session = session

    def create_item(self, title, prod_year):
        new_item = MediaItem(title, prod_year, len(title))
        self.item_repo.add(self.session, new_item)
        self.session.commit()



if __name__ == '__main__':
    username='amirrot'
    password='RPzc9yQh'
    connection_string = f"mssql+pyodbc://{username}:{password}@132.72.64.124/{username}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    engine = create_engine(connection_string)

    # with engine.connect() as connection:
    #     # Step 1: Drop all foreign key constraints
    #     connection.execute(text("""
    #         DECLARE @sql NVARCHAR(MAX) = N'';
    #         SELECT @sql += 'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + '].[' + OBJECT_NAME(parent_object_id) + '] DROP CONSTRAINT [' + name + '];'
    #         FROM sys.foreign_keys;
    #         EXEC sp_executesql @sql;
    #     """))
    #
    #     # Step 2: Drop all tables
    #     connection.execute(text("""
    #         DECLARE @sql NVARCHAR(MAX) = N'';
    #         SELECT @sql += 'DROP TABLE [' + SCHEMA_NAME(schema_id) + '].[' + name + '];'
    #         FROM sys.tables;
    #         EXEC sp_executesql @sql;
    #     """))
    #
    #     print("All tables and foreign key constraints dropped.")

    Base.metadata.create_all(engine)  # Create the tables in the database

    session = sessionmaker(bind=engine)()

    item_service = ItemService(session, ItemRepository())
    # csv_file_path = "../hw1/films.csv"
    #
    # # Open the CSV file and process it
    # with open(csv_file_path, mode='r', encoding='utf-8') as file:
    #     reader = csv.reader(file)  # Read rows as lists
    #
    #     for row in reader:
    #         title = row[0]  # First column is the title
    #         prod_date = int(row[1])  # Second column is the production date
    #
    #         # Call the item_service to create items
    #         item_service.create_item(title, prod_date)
    #
    # print("Items created successfully from the CSV.")
    user_service = UserService(session, UserRepository())

    # # add user
    usernames = ['user1', 'user2', 'user3', 'user4', 'user5']
    # passwords = ['12345', '67890', '11223', '44556', '78901']  # Numeric strings for passwords
    # first_names = ['John', 'Jane', 'Alice', 'Bob', 'Charlie']
    # last_names = ['Smith', 'Doe', 'Brown', 'Johnson', 'Taylor']
    #
    # # Create users
    # for i in range(5):
    #     username = usernames[i]
    #     password = passwords[i]
    #     first_name = first_names[i]
    #     last_name = last_names[i]
    #     # Set a different date of birth for each user
    #     date_of_birth = datetime(1990, 1, 1) + timedelta(days=i * 365)  # Increment by 1 year for each user
    #     # Call the create_user method
    #     user_service.create_user(
    #         username=username,
    #         password=password,
    #         first_name=first_name,
    #         last_name=last_name,
    #         date_of_birth=date_of_birth,
    #     )
    #
    # print("5 new employees created successfully!")

    print(user_service.validateUser(username='amirrot', password='12358864'))
    print(user_service.validateUser(username='user1', password='12345'))
    print(user_service.validateUser(username='user1', password='12445'))


    # user_service.add_history_to_user(username='user1', media_item_id=1)
    # user_service.add_history_to_user(username='user2', media_item_id=68)
    # user_service.add_history_to_user(username='user2', media_item_id=69)
    # user_service.add_history_to_user(username='user3', media_item_id=98)
    # user_service.add_history_to_user(username='user3', media_item_id=70)
    # user_service.add_history_to_user(username='user4', media_item_id=71)
    # user_service.add_history_to_user(username='user4', media_item_id=82)
    # user_service.add_history_to_user(username='user4', media_item_id=83)
    # user_service.add_history_to_user(username='user4', media_item_id=84)
    # user_service.add_history_to_user(username='user5', media_item_id=98)
    # user_service.add_history_to_user(username='user5', media_item_id=70)

    print(user_service.getNumberOfRegistredUsers(1))
    print(user_service.getNumberOfRegistredUsers(2))

    print(user_service.get_all_users())
    print(item_service.item_repo.getTopNItems(session, 10))
    for username in usernames:
        print(f'{username}:{user_service.sum_title_length_to_user(username)}')
    x=0

