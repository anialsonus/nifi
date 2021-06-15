docker stop ES01
docker stop ES02
docker stop SQUID
docker stop SQUID_AUTH
docker rm ES01
docker rm ES02
docker rm SQUID
docker rm SQUID_AUTH
docker network rm es_squid