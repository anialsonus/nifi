docker pull sameersbn/squid:3.5.27-2
echo docker run --name SQUID_NIFI -d --restart=always --publish $1:3228 --volume $3/squid.conf:/etc/squid/squid.conf  --volume /srv/docker/squid/cache:/var/spool/squid --network=$4 sameersbn/squid:3.5.27-2
echo docker run --name SQUID_NIFI_AUTH -d --restart=always --publish $2:3328 --volume $3/squid-auth.conf:/etc/squid/squid.conf  --volume /srv/docker/squid/cache:/var/spool/squid --network=$4 sameersbn/squid:3.5.27-2
docker run --name SQUID_NIFI -d --restart=always --publish $1:3228 --volume $3/squid.conf:/etc/squid/squid.conf  --volume /srv/docker/squid/cache:/var/spool/squid --network=$4 sameersbn/squid:3.5.27-2
docker run --name SQUID_NIFI_AUTH -d --restart=always --publish $2:3328 --volume $3/squid-auth.conf:/etc/squid/squid.conf  --volume /srv/docker/squid/cache:/var/spool/squid --network=$4 sameersbn/squid:3.5.27-2
