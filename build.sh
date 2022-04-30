
# build image
docker build -t mina-graphql .

# tag image
docker tag mina-graphql localhost:5000/mina-graphql

# push image
docker push localhost:5000/mina-graphql:latest
