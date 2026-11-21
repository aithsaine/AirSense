# AirSense
une plateforme complète de surveillance de la qualité de l'air qui traite des données de capteurs IoT en streaming, détecte des anomalies, entraîne des modèles prédictifs, et rend les données accessibles via une interface de visualisation et un catalogue



SetUp :

Etape1 : 
    installer les bibiotheques dans requirements.txt



Etape2 : 
    Cree les trois topics (air_data, processed_data, anomalies)

    docker exec -it dataops-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic air_data --partitions 3 --replication-factor 1

    docker exec -it dataops-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic processed_data --partitions 3 --replication-factor 1

    docker exec -it dataops-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic anomalies --partitions 3 --replication-factor 1