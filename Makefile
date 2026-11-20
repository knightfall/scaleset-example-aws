GOLANGCI_LINT_VERSION := v2.15.0

.PHONY: check check-go check-cdk

check: check-go check-cdk

check-go:
	@set -eu; \
		expected="go$$(awk '$$1 == "go" {print $$2}' go.mod)"; \
		actual="$$(go env GOVERSION)"; \
		test "$$actual" = "$$expected" || { printf 'Expected %s, found %s\n' "$$expected" "$$actual" >&2; exit 1; }; \
		go mod download; \
		go mod verify; \
		formatter="$$(go env GOROOT)/bin/gofmt"; \
		unformatted="$$(find . -type d \( -name .git -o -name node_modules -o -name vendor \) -prune \
			-o -type f -name '*.go' -exec "$$formatter" -l {} +)"; \
		test -z "$$unformatted" || { printf 'Run gofmt on:\n%s\n' "$$unformatted" >&2; exit 1; }; \
		packages="$$(go list ./...)"; \
		if test -n "$$packages"; then \
			go vet ./...; \
			go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run; \
			go test -race ./...; \
		else \
			printf 'No Go packages yet. Verified the toolchain and module checksums.\n'; \
		fi

check-cdk:
	@set -eu; \
		expected="v$$(cat .node-version)"; \
		actual="$$(node --version)"; \
		test "$$actual" = "$$expected" || { printf 'Expected Node %s, found %s\n' "$$expected" "$$actual" >&2; exit 1; }
	npm --prefix cdk ci --ignore-scripts
	npm --prefix cdk run typecheck
	npm --prefix cdk run lint
	npm --prefix cdk run format:check
	npm --prefix cdk test
