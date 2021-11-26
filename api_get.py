##Producer 
from time import sleep
import json
from json import dumps
from kafka import KafkaProducer
import requests
import random
url="https://db.ygoprodeck.com/api/v7/cardinfo.php"
response = requests.get(url)
##statut de la requete
print(response.status_code)
a=response.json()
##pour avoir la liste des cartes
data=a['data']
random.shuffle(data)
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
for card in data:
    ##On envoie pas les cartes monstres link car elles n'ont pas de points de defenses 
    ##On envoie pas les cartes qui n'ont pas de card_sets 
    if(card['type']!='Link Monster') and ('card_sets' in card):
        producer.send('Cards',json.dumps(card).encode('utf-8'))
        print('envoyé')
        print(card)
        producer.flush()
        sleep(2)
