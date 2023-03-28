run:
	go run ./cmd/main.go 

redis_up:
	docker-compose -f redis.yml up -d	

redis_down:
	docker-compose -f redis.yml down	

PONY: run redis_up redis_down
