import time


class View:
    def show_menu(self):
        while True:
            print("Menu:")
            print("1. Output table names")
            print("2. Output table column names")
            print("3. Entering data into a table")
            print("4. Editing data in a table")
            print("5. Extracting data in a table")
            print("6. Generating random data into a table")
            print("7. Exit")

            choice = input("Make a choice: ")

            if choice in ('1', '2', '3', '4', '5', '6', '7'):
                return choice
            else:
                print("Enter the correct option number (1 to 7)")
                time.sleep(2)

    def show_message(self, message):
        print(message)
        time.sleep(2)

    def ask_continue(self):
        agree = input("Continue making changes? (y/n) ")
        return agree

    def show_tables(self, tables):
        print("Table names:")
        for table in tables:
            print(table)
        time.sleep(2)

    def ask_table(self):
        table_name = input("Enter the table name: ")
        return table_name

    def show_columns(self, columns):
        print("Column names:")
        for column in columns:
            print(column)
        time.sleep(2)

    def insert(self):
        while True:
            try:
                table = input("Enter the table name: ")
                columns = input("Enter column names (separated by a space): ").split()
                val = input("Enter the appropriate values (separated by a space): ").split()

                if len(columns) != len(val):
                    raise ValueError("The number of columns must be equal to the number of values")

                return table, columns, val
            except ValueError as e:
                print(f"Помилка: {e}")

    def update(self):
        while True:
            try:
                table = input("Enter the table name: ")
                column = input("Enter the name of the column you want to change: ")
                id = int(input("Enter the ID of the row you want to change: "))
                new_value = input("Enter a new value: ")
                return table, column, id, new_value
            except ValueError as e:
                print(f"Помилка: {e}")

    def delete(self):
        while True:
            try:
                table = input("Enter the table name: ")
                id = int(input("Enter the ID of the row you want to delete: "))
                return table, id
            except ValueError as e:
                print(f"Помилка: {e}")

    def generate_data_input(self):
        while True:
            try:
                table_name = input("Enter the table name: ")
                num_rows = int(input("Enter the number of rows to generate: "))
                return table_name, num_rows
            except ValueError as e:
                print(f"Помилка: {e}")
