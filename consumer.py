import pika
import json
import mysql.connector
from mysql.connector import Error

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection
    
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


# creating connection to database        
db_connection = create_db_connection("localhost","root","root","task")
query = "INSERT INTO served (drink,name) VALUES ('{0}','{1}')" # query to insert values in database


# connecting to RabbitMQ to read message from queue
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', pika.PlainCredentials("guest", "guest")))
channel = connection.channel()

def callback(ch, method, properties, body):
    body = json.loads(body) # message body from the queue
    drink = body['drink_detail']['drinks'][0]['strDrink']
    name = body['user']['results'][0]['name']['first']
    execute_query(db_connection,query.format(drink,name)) # executing the sql query
    print(f'{name} received {drink}')
    
channel.basic_consume(queue="my_app", on_message_callback=callback, auto_ack=True)
channel.start_consuming() # checking for messages in queue and callig callback function on receiving it
