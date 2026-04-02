PKG := github.com/goccy/go-zetasqlite

# Same dev image and LOCAL_GO_CACHE_ROOT as go-zetasql (see ../go-zetasql/Makefile).
GO_ZETASQL_ROOT ?= $(abspath $(CURDIR)/../go-zetasql)
DOCKER_DEV_IMAGE ?= go-zetasql:dev
LOCAL_GO_CACHE_ROOT ?= $(HOME)/.cache/go-zetasql

GOBIN := $(CURDIR)/bin
PKGS := $(shell go list ./... | grep -v cmd | grep -v benchmarks )
COVER_PKGS := $(foreach pkg,$(PKGS),$(subst $(PKG),.,$(pkg)))

COMMA := ,
EMPTY :=
SPACE := $(EMPTY) $(EMPTY)
COVERPKG_OPT := $(subst $(SPACE),$(COMMA),$(COVER_PKGS))

$(GOBIN):
	@mkdir -p $(GOBIN)

.PHONY: build
build:
	cd ./cmd/zetasqlite-cli && go build .

.PHONY: cover
cover:
	go test -coverpkg=$(COVERPKG_OPT) -coverprofile=cover.out ./...

.PHONY: cover-html
cover-html: cover
	go tool cover -html=cover.out

.PHONY: lint
lint: lint/install
	$(GOBIN)/golangci-lint run --timeout 30m

lint/install: | $(GOBIN)
	# binary will be $(go env GOPATH)/bin/golangci-lint
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b $(GOBIN) v2.4.0

# Run tests in the same go-zetasql:dev toolchain + shared GOCACHE as ../go-zetasql (build the image there first: make docker/build-dev).
.PHONY: test/linux
test/linux:
	docker run --rm \
		-e CGO_ENABLED=1 -e CC=clang -e CXX=clang++ \
		-e CCACHE_DIR=/root/.ccache -e CCACHE_COMPRESS=1 \
		-v "$(CURDIR)":/work/go-zetasqlite \
		-v "$(GO_ZETASQL_ROOT)":/work/go-zetasql \
		-v "$(LOCAL_GO_CACHE_ROOT)/gocache":/root/.cache/go-build \
		-v "$(LOCAL_GO_CACHE_ROOT)/gomodcache":/go/pkg/mod \
		-v "$(LOCAL_GO_CACHE_ROOT)/ccache":/root/.ccache \
		-w /work/go-zetasqlite \
		$(DOCKER_DEV_IMAGE) \
		bash -c "go test -race -v ./... -count=1"
