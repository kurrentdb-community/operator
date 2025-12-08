IMG ?= ghcr.io/kurrentdb-community/operator:latest
KUBECTL ?= kubectl

.PHONY: help
help:
	@echo "Targets:"
	@echo "  apply-all       Apply CRD, RBAC and Manager manifests"
	@echo "  delete-all      Delete CRD, RBAC and Manager manifests"
	@echo "  sample-basic    Apply the basic sample KurrentCluster"
	@echo "  sample-advanced Apply the advanced sample KurrentCluster (if present)"
	@echo "  delete-samples  Delete sample KurrentClusters"
	@echo "  docker-build    Build the operator image"
	@echo "  docker-push     Push the operator image"

.PHONY: apply-all
apply-all:
	$(KUBECTL) apply -f config/manager/manager.yaml
	$(KUBECTL) apply -f config/rbac/
	$(KUBECTL) apply -f config/crd/

.PHONY: delete-all
delete-all:
	-$(KUBECTL) delete -f config/manager/manager.yaml --ignore-not-found=true
	-$(KUBECTL) delete -f config/rbac/ --ignore-not-found=true
	-$(KUBECTL) delete -f config/crd/ --ignore-not-found=true

.PHONY: sample-basic
sample-basic:
	$(KUBECTL) apply -f config/samples/kurrent_v1_kurrentcluster.yaml

.PHONY: sample-advanced
sample-advanced:
	-@[ -f config/samples/kurrent_v1_kurrentcluster_advanced.yaml ] && $(KUBECTL) apply -f config/samples/kurrent_v1_kurrentcluster_advanced.yaml || echo "Advanced sample not present"

.PHONY: delete-samples
delete-samples:
	-$(KUBECTL) delete -f config/samples/kurrent_v1_kurrentcluster.yaml --ignore-not-found=true
	-@[ -f config/samples/kurrent_v1_kurrentcluster_advanced.yaml ] && $(KUBECTL) delete -f config/samples/kurrent_v1_kurrentcluster_advanced.yaml --ignore-not-found=true || true

.PHONY: docker-build
docker-build:
	docker build -t $(IMG) .

.PHONY: docker-push
docker-push:
	docker push $(IMG)
