docker network create --subnet=172.18.0.0/16 --gateway=172.18.0.1 es_squid
docker run -e ES_JAVA_OPTS="-Xms256m -Xmx256m" -d -p $1:$3 -v $6/es1.yml:/usr/share/elasticsearch/config/elasticsearch.yml  -v data01:/usr/share/elasticsearch/data --network=es_squid --ip 172.18.0.2 --name ES01 elasticsearch:7.4.2
docker run -e ES_JAVA_OPTS="-Xms256m -Xmx256m" -d -p $2:$3 -v $6/es2.yml:/usr/share/elasticsearch/config/elasticsearch.yml  -v data02:/usr/share/elasticsearch/data --network=es_squid --ip 172.18.0.3 --name ES02 elasticsearch:7.4.2
docker run --name SQUID -d --restart=always --publish $4:3228 --volume $6/squid.conf:/etc/squid/squid.conf  --volume /srv/docker/squid/cache:/var/spool/squid --network=es_squid sameersbn/squid:3.5.27-2
docker run --name SQUID_AUTH -d --restart=always --publish $5:3328 --volume $6/squid-auth.conf:/etc/squid/squid.conf  --volume /srv/docker/squid/cache:/var/spool/squid --network=es_squid sameersbn/squid:3.5.27-2
