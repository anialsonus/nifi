docker pull elasticsearch:2.1.0
echo docker run --name ES_NIFI_01 --network $8 --ip $5 -v $9/es1.yml:/usr/share/elasticsearch/config/elasticsearch.yml  -v es_nifi_01:/usr/share/elasticsearch/data -d -p $1:9200 -p $3:9300 elasticsearch:2.1.0  -Des.network.bind_host=$5 -Des.network.publish_host=$5 -Des.discovery.zen.ping.unicast.hosts=$7
echo docker run --name ES_NIFI_02 --network $8 --ip $6 -v $9/es2.yml:/usr/share/elasticsearch/config/elasticsearch.yml  -v es_nifi_02:/usr/share/elasticsearch/data -d -p $2:9200 -p $4:9300 elasticsearch:2.1.0  -Des.network.bind_host=$6 -Des.network.publish_host=$6 -Des.discovery.zen.ping.unicast.hosts=$7
docker run --name ES_NIFI_01 --network $8 --ip $5 -v $9/es1.yml:/usr/share/elasticsearch/config/elasticsearch.yml  -v es_nifi_01:/usr/share/elasticsearch/data -d -p $1:9200 -p $3:9300 elasticsearch:2.1.0  -Des.network.bind_host=$5 -Des.network.publish_host=$5 -Des.discovery.zen.ping.unicast.hosts=$7
docker run --name ES_NIFI_02 --network $8 --ip $6 -v $9/es2.yml:/usr/share/elasticsearch/config/elasticsearch.yml  -v es_nifi_02:/usr/share/elasticsearch/data -d -p $2:9200 -p $4:9300 elasticsearch:2.1.0  -Des.network.bind_host=$6 -Des.network.publish_host=$6 -Des.discovery.zen.ping.unicast.hosts=$7
