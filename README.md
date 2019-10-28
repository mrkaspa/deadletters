# Run Dev

Ensure to have a rabbitmq installed

```
docker run -d --hostname my-rabbit --name some-rabbit -p 15672:15672 -p 5672:5672 rabbitmq:3-management
```

And a mongodb instance

```
sudo apt install mongodb
```

# Deploy

The app comes in two modes, the server

```
docker build -f server/Dockerfile .
```

And a CLI to run as a cron job

```
docker build -f retry/Dockerfile .
```

# Testing

Ensure to run first the server and after that run the main in the example folder, this will simulate a amqp queue listener and reject messages going to it in order to be sent to the dlx listener
