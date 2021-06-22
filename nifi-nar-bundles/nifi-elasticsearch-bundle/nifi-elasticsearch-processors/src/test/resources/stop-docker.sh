docker stop ES_NIFI_01
docker stop ES_NIFI_02
docker stop SQUID_NIFI
docker stop SQUID_NIFI_AUTH
docker rm ES_NIFI_01
docker rm ES_NIFI_01
docker rm SQUID_NIFI
docker rm SQUID_NIFI_AUTH
docker volume rm es_nifi_01
docker volume rm es_nifi_02
docker network rm es_squid
