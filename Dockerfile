# ---- build stage ----
FROM golang:1.22-alpine AS build
WORKDIR /src

# Cache module downloads (this project has no external deps, but this keeps the
# layer stable if any are added later).
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/godb-server ./cmd/godb-server \
 && CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/godb-cli    ./cmd/godb-cli

# ---- runtime stage ----
FROM alpine:3.20
RUN adduser -D -H godb
COPY --from=build /out/godb-server /usr/local/bin/godb-server
COPY --from=build /out/godb-cli    /usr/local/bin/godb-cli

VOLUME /data
EXPOSE 6380
USER godb
ENTRYPOINT ["godb-server", "-addr", ":6380", "-dir", "/data"]
