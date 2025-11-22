import sqlite3

connection = sqlite3.connect('users.db')
connection = sqlite3.connect(':memory:')

# You can then create a cursor object to execute SQL commands
cursor = connection.cursor()

# ... perform database operations using the cursor ...
#TODO: append acivity log (total steps, common routes, total walk time)

# Commit changes to the database
connection.commit()

# Close the connection when done
connection.close()
