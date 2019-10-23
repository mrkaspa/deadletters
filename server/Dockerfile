FROM golang:1.13.0-stretch
LABEL mainainter = "Michel Perez michel@liftit.co"

RUN mkdir -p $GOPATH/liftitapp/dlserver
WORKDIR $GOPATH/liftitapp/dlserver
COPY . .

RUN export GO111MODULE=on
WORKDIR $GOPATH/liftitapp/dlserver/server
RUN go build -o main
RUN mkdir /app && cp main /app

ENTRYPOINT [ "/app/main" ]
CMD ./main
