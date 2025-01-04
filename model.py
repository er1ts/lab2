from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Numeric, Date, SmallInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import select, update, delete, insert
import psycopg2

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    __table_args__ = {'schema': 'public'}

    user_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), nullable=False)
    address = Column(String(100), nullable=False)

    rentals = relationship("Rental", back_populates="user")
    reviews = relationship("Review", back_populates="user")


class Equipment(Base):
    __tablename__ = 'equipment'
    equipment_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    description = Column(String)
    price_per_day = Column(Numeric, nullable=False)


class Rental(Base):
    __tablename__ = 'rental'
    __table_args__ = {'schema': 'public'}

    rental_id = Column(Integer, primary_key=True)
    equipments_id = Column(Integer, ForeignKey('public.equipments.equipments_id'), nullable=False)
    user_id = Column(Integer, ForeignKey('public.users.user_id'), nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    total_price = Column(Integer, nullable=False)

    user = relationship("User", back_populates="rentals")
    equipment = relationship("Equipment", back_populates="rentals")


class Review(Base):
    __tablename__ = 'review'
    __table_args__ = {'schema': 'public'}

    review_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('public.users.user_id'), nullable=False)
    equipments_id = Column(Integer, ForeignKey('public.equipments.equipments_id'), nullable=False)
    rating = Column(SmallInteger, nullable=False)
    comment = Column(String, nullable=True)

    user = relationship("User", back_populates="reviews")
    equipment = relationship("Equipment", back_populates="reviews")


class Model:
    def __init__(self):
        self.engine = create_engine('postgresql://postgres:1111@localhost:5432/public')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        self.conn = psycopg2.connect(
            host="localhost",
            database="public",
            user="postgres",
            password="qwerty123 ",
            options="-c client_encoding=utf8"
        )

    def get_all_tables(self):
        return Base.metadata.tables.keys()

    def get_all_columns(self, table_name):
        return Base.metadata.tables[table_name].columns.keys()

    def add_data(self, table_name, columns, val):
        try:
            insert_query = insert(Base.metadata.tables[table_name]).values(dict(zip(columns, val)))
            self.session.execute(insert_query)
            self.session.commit()
            return 1
        except Exception as e:
            print(e)
            return 0

    def update_data(self, table_name, column, id, new_value):
        try:
            # Split the table_name into schema and table parts
            schema_name, table_name = table_name.split('.')

            # Get the table from the MetaData object
            table = Base.metadata.tables.get(f'{schema_name}.{table_name}')

            if table is None:
                print(f"Таблиця '{table_name}' не знайдена на схемі '{schema_name}'")
                return 0

            # Construct the update query
            update_query = update(table). \
                where(table.columns[f'{table_name.lower()}_id'] == id). \
                values({column: new_value})

            # Execute the query and commit the changes
            self.session.execute(update_query)
            self.session.commit()

            return 1
        except Exception as e:
            print(e)
            return 0

    def delete_data(self, table_name, id):
        try:
            # Split the table_name into schema and table parts
            schema_name, table_name = table_name.split('.')

            # Get the table from the MetaData object
            table = Base.metadata.tables.get(f'{schema_name}.{table_name}')

            if table is None:
                print(f"Таблиця '{table_name}' не знайдена на схемі '{schema_name}'")
                return 0

            # Construct the delete query
            delete_query = delete(table). \
                where(table.columns[f'{table_name.lower()}_id'] == id)

            # Execute the query and commit the changes
            self.session.execute(delete_query)
            self.session.commit()

            return 1
        except Exception as e:
            print(e)
            return 0

    def generate_data(self, table_name, count):
        schema_name, table_name = table_name.split('.')

        c = self.conn.cursor()
        c.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (table_name,))
        columns_info = c.fetchall()

        # Знайдемо назву ключового поля
        id_column = f'{table_name.lower()}_id'
        if table_name == 'Teacher_Subject':
            id_column = 'tab_id'

        # Генеруємо значення для всіх інших полів
        for i in range(count):
            insert_query = f'INSERT INTO "public"."{table_name}" ('
            select_subquery = ""

            for column_info in columns_info:
                column_name = column_info[0]
                column_type = column_info[1]

                if column_name == id_column:
                    c.execute(f'SELECT max("{id_column}") FROM "public"."{table_name}"')
                    max_id = c.fetchone()[0] or 0
                    select_subquery += f'{max_id + 1},'
                elif column_name.endswith('_id'):
                    related_table_name = column_name[:-3].capitalize()
                    # Знаходимо існуючий id з відповідної таблиці
                    c.execute(f'SELECT {related_table_name.lower()}_id FROM "public"."{related_table_name}" ORDER '
                              f'BY RANDOM() LIMIT 1')
                    related_id = c.fetchone()[0]
                    select_subquery += f'{related_id},'
                elif column_type == 'integer':
                    select_subquery += f'trunc(random()*100)::INT,'
                elif column_type == 'character varying':
                    select_subquery += f"'Text {column_name}',"
                elif column_type == 'date':
                    select_subquery += "'2022-01-01',"
                elif column_type == 'timestamp with time zone':
                    select_subquery += "'2022-01-01 08:30:00+03',"
                else:
                    continue

                insert_query += f'"{column_name}",'

            insert_query = insert_query.rstrip(',') + f') VALUES ({select_subquery[:-1]})'
            c.execute(insert_query)

        self.conn.commit()
