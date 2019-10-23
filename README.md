# Run Dev

Ensure to have a rabbitmq installed

```
docker run -d --hostname my-rabbit --name some-rabbit -p 15672:15672 -p 5672:5672 rabbitmq:3-management
```

And a mongodb instance



# Deploy

The app comes in two modes, the server

```
docker build -f server/Dockerfile .
```

And a CLI to run as a cron job

```
docker build -f retry/Dockerfile .
```
