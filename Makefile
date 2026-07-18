DOCKER_COMPOSE = sudo docker compose
MAVEN          = mvn

# Alvos Virtuais
.PHONY: all build-mvn build-docker build-all up down reset logs logs-sql flink-list ps

# Alvo padrão: Compila o Maven e sobe a infraestrutura
all: build-all up

# Compila os conectores e gera os JARs na pasta target/flink-lib
build-mvn:
	$(MAVEN) clean package

# Reconstrói a imagem do Docker 
build-docker:
	$(DOCKER_COMPOSE) build --no-cache

# Executa o pipeline de build completo (Java + Docker)
build-all: build-mvn build-docker

# Sobe os containers em background
up:
	$(DOCKER_COMPOSE) up -d
	@echo "🚀 Stack Finstream ativa! Acede a http://localhost:8081 para a Web UI."

# Derruba a infraestrutura completa
down:
	$(DOCKER_COMPOSE) down

# Interrope os jobs
stop:
	$(DOCKER_COMPOSE) stop
	@echo "🛑 Containers parados com segurança. Estado preservado."

# Limpeza pesada: Derruba tudo, limpa cache de rede e recria os containers
reset:
	$(DOCKER_COMPOSE) down
	$(DOCKER_COMPOSE) up -d --force-recreate

# Mostra o estado atual dos containers
ps:
	$(DOCKER_COMPOSE) ps

# Acompanha os logs em tempo real (Geral)
logs:
	$(DOCKER_COMPOSE) logs -f

# Acompanha especificamente o output do SQL Client para ver os submits de Jobs
logs-sql:
	$(DOCKER_COMPOSE) logs -f flink-sql-client

# Lista os Streaming Jobs que estão a correr no cluster Flink
flink-list:
	$(DOCKER_COMPOSE) exec -it jobmanager flink list
