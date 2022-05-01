
# build image
docker build -t mina-graphql .

# tag image
docker tag mina-graphql 192.168.4.2:5050/mina-graphql

# push image
docker push 192.168.4.2:5050/mina-graphql:latest
