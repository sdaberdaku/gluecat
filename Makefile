# Local shortcuts. JAVA_HOME is used as set in the environment (JDK 17 or later builds every module).
MODULE ?= spark-4.1_2.13
BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)
MINISTACK_IMAGE := ministackorg/ministack:1.5.31
COMPOSE := docker compose -f docker/docker-compose.yml

.PHONY: test verify package ministack-up ministack-down compat-up compat-down compat matrix clean

# Unit tests of one module.
test:
	./mvnw -pl modules/$(MODULE) -DskipITs test

# Unit tests, the jar, and integration tests against the jar; needs MiniStack (make ministack-up).
verify:
	./mvnw -pl modules/$(MODULE) verify

# Jars of every module, no tests.
package:
	./mvnw -DskipTests -DskipITs package

ministack-up:
	docker run -d --rm --name ministack -p 4566:4566 $(MINISTACK_IMAGE)

ministack-down:
	docker rm -f ministack

# MiniStack, Trino and legacy Spark containers for the compatibility suites (run make ministack-down first).
compat-up:
	mkdir -m 0777 -p /tmp/gluecat-compat && $(COMPOSE) up -d --wait

compat-down:
	$(COMPOSE) down

compat:
	TRINO_URL=jdbc:trino://localhost:8080 LEGACY_SPARK_CONTAINER=legacy-spark \
		./mvnw -q -pl modules/$(MODULE) -Dit.test='TrinoCompat*IT,LegacyHiveGlueCompat*IT' verify

# Full CI matrix on GitHub Actions for the current branch (or BRANCH=...), which must be pushed.
matrix:
	@branch='$(BRANCH)'; \
	if [ -z "$$branch" ] || [ "$$branch" = HEAD ]; then \
		echo 'make matrix: no branch checked out (detached HEAD); run make matrix BRANCH=<pushed branch>' >&2; exit 1; fi; \
	url=$$(gh workflow run ci.yml --ref "$$branch" -f full=true | grep -o 'https://[^ ]*/actions/runs/[0-9]*' | tail -n 1); \
	if [ -z "$$url" ]; then \
		echo 'make matrix: gh workflow run printed no run URL; find the run with gh run list --workflow ci.yml' >&2; exit 1; fi; \
	echo "$$url"; gh run watch --exit-status "$${url##*/}"

clean:
	./mvnw -q clean
