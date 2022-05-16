
# build image
docker build -t mina-graphql-subscribe .

# tag image
docker tag mina-graphql-subscribe makalfe/mina-graphql-subscribe

# push image
docker push makalfe/mina-graphql-subscribe:latest
