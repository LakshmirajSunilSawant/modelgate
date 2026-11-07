IMG_REGISTRY ?= ghcr.io/modelgate
TAG ?= dev
KIND_CLUSTER ?= modelgate
CONTROLLER_GEN ?= controller-gen

.PHONY: all generate manifests test test-short envtest bench loadtest build images kind-up kind-load deploy demo smoke e2e e2e-chaos e2e-replay vulncheck

all: generate manifests test build

generate:
	$(CONTROLLER_GEN) object:headerFile=hack/boilerplate.go.txt paths=./api/v1alpha1

manifests:
	$(CONTROLLER_GEN) crd:allowDangerousTypes=true,generateEmbeddedObjectMeta=true paths=./api/v1alpha1 output:crd:artifacts:config=config/crd
	$(CONTROLLER_GEN) rbac:roleName=modelgate-controller paths=./internal/controller output:rbac:artifacts:config=config/rbac
	cp config/crd/*.yaml charts/modelgate/crds/

test:
	go vet ./...
	go test ./...

ENVTEST_K8S_VERSION ?= 1.33.0
envtest:
	go install sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.21
	KUBEBUILDER_ASSETS="$$(setup-envtest use $(ENVTEST_K8S_VERSION) -p path)" go test ./internal/controller/ ./internal/webhook/ -run "Envtest|Webhook" -v

bench:
	python demo/model/export_pool.py --out bench/pool.json
	go run ./hack/benchmark -pool bench/pool.json -runs 50

loadtest:
	go run ./hack/loadtest -rps 1000 -duration 30s

# End-to-end on kind: NATS, Postgres audit log, load through Envoy Gateway.
e2e:
	CLUSTER=$(KIND_CLUSTER) ./hack/e2e/run.sh

# The same with Chaos Mesh killing the controller, analyzer and NATS.
e2e-chaos:
	CLUSTER=$(KIND_CLUSTER) CHAOS=mesh ./hack/e2e/run.sh

# No live traffic: only replayed recorded requests feed the analysis.
e2e-replay:
	CLUSTER=$(KIND_CLUSTER) RPS=0 REPLAY=1 SCENARIOS="identical:promote swapped:rollback" ./hack/e2e/run.sh

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

test-short:
	go test -short ./...

build:
	go build -o bin/ ./cmd/...

images:
	docker build --build-arg CMD=manager  -t $(IMG_REGISTRY)/modelgate-controller:$(TAG) .
	docker build --build-arg CMD=analyzer -t $(IMG_REGISTRY)/modelgate-analyzer:$(TAG) .
	docker build --build-arg CMD=sidecar  -t $(IMG_REGISTRY)/modelgate-sidecar:$(TAG) .
	docker build -t modelgate-demo-model:dev demo/model

kind-up:
	./hack/kind-up.sh $(KIND_CLUSTER)

kind-load: images
	for i in modelgate-controller modelgate-analyzer modelgate-sidecar; do \
	  kind load docker-image $(IMG_REGISTRY)/$$i:$(TAG) --name $(KIND_CLUSTER); done
	kind load docker-image modelgate-demo-model:dev --name $(KIND_CLUSTER)

deploy:
	helm upgrade --install modelgate charts/modelgate -n modelgate-system --create-namespace \
	  --set image.tag=$(TAG) --set image.pullPolicy=IfNotPresent

demo:
	kubectl create namespace demo --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f config/samples/demo.yaml

smoke: build
	python hack/local_smoke.py --variant good
	python hack/local_smoke.py --variant swapped
