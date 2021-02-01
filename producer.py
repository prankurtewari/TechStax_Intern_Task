import requests
import json
from timeit import default_timer
import asyncio
import pika


def request(session):
    
    url1 = "https://www.thecocktaildb.com/api/json/v1/1/random.php"
    url2 = "https://randomuser.me/api/"
    
    # GET request 
    with session.get(url1) as response:
        response1 = response.json()
        
    with session.get(url2) as response:
        response2 = response.json()
        
    # combining response from both the APIs
    final_response = {'drink_detail':response1,'user':response2}
    
    
    # connecting to RabbitMQ to publish message in queue
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', pika.PlainCredentials('guest', 'guest')))
    channel = connection.channel()
    
    
    #publishing message
    channel.basic_publish(exchange='my_exchange', routing_key='test', body=json.dumps(final_response))

    connection.close()
    
    
    
async def start():
    with requests.session() as session:
        while True:
            request(session)
            await asyncio.sleep(5) # async call in every 5 seconds
            
            
def stop():
    task.cancel()
               
# creating async loop 
loop = asyncio.get_event_loop()
loop.call_later(120,stop) # running loop for 120 seconds 
task = loop.create_task(start())

try:
    loop.run_until_complete(task)
except asyncio.CancelledError:
    pass
