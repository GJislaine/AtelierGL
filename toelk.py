import psycopg2
from elasticsearch import Elasticsearch
#executer elasticsearfch.bat d'abord et attendre que localhost:9200 s'ouvre 
# Paramètres de connexion à la base de données PostgreSQL
parametres_pg = {
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'jisjis12'
}
# Paramètres de connexion à Elasticsearch
parametres_es = {
    'host': 'localhost',
    'port': 9200,
    'scheme': 'http',
    'use_ssl': False
    }
# Création de la connexion à PostgreSQL
try:
  connexion_pg = psycopg2.connect(**parametres_pg)
  curseur_pg = connexion_pg.cursor()
  # Création de la connexion à Elasticsearch
  # username EL: elastic
  # mdp EL : J69MY7mgo3UVCi8Obgaa
  connexion_es = Elasticsearch(hosts=[parametres_es],
    basic_auth=('elastic', 'J69MY7mgo3UVCi8Obgaa') )
  # Exécution de la requête pour extraire les données de PostgreSQL
  curseur_pg.execute("SELECT DISTINCT * FROM entreprises")
  resultats = curseur_pg.fetchall()
  # Boucle sur les résultats et envoi des données à Elasticsearch
  for resultat in resultats:
     donnees = {
        'name': resultat[0],
        'adresse': resultat[1],
        'numero': resultat[2],
        'loca': resultat[3],
        'site': resultat[4],
        'fct': resultat[5],
        }
    # Indexation des données dans Elasticsearch
     connexion_es.index(index='idex',  id=resultat[0],document=donnees)
  print("Transfert des données réussi !")
except Exception as e:
     print(f"Erreur lors du transfert des données : {e}")
finally:
    # Fermeture des connexions
    if  curseur_pg is not None:
         curseur_pg.close()
    if  connexion_pg is not None:
         connexion_pg.close()
