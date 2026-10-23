# Local shortcuts. JAVA_HOME is used as set in the environment (JDK 17 or later builds every module).
MODULE ?= spark-4.1_2.13
BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)
MINISTACK_IMAGE := ministackorg/ministack:1.5.31
COMPOSE := docker compose -f docker/docker-compose.yml

.PHONY: test verify package ministack-up ministack-down compat-up compat-down compat matrix docs clean

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

# MiniStack, Trino and legacy Spark containers for the compatibility suites; replaces the ministack-up container.
compat-up:
	-docker rm -f ministack >/dev/null 2>&1
	mkdir -m 0777 -p /tmp/gluecat-compat && $(COMPOSE) up -d --wait

# Stops them and empties /tmp/gluecat-compat as root (the containers write there as other users).
compat-down:
	$(COMPOSE) down
	docker run --rm --user 0 --entrypoint sh -v /tmp/gluecat-compat:/w $(MINISTACK_IMAGE) -c 'rm -rf /w/*'

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

# Serves the documentation site (pip installs the pinned mkdocs into the active Python environment).
docs:
	mkdir -p docs/site/assets && cp docs/logo/gluecat-icon.png docs/logo/gluecat-banner.jpg docs/site/assets/ && cp CHANGELOG.md docs/site/changelog.md
	pip install -r docs/site/requirements.txt && mkdocs serve

clean:
	./mvnw -q clean
