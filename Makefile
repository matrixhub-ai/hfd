

.PHONY: serve-web
serve-web: ## Run web frontend locally
	make -C web serve

.PHONY: serve-api
serve-api: ## Serve the API only
	go run ./cmd/gitd

.PHONY: run
run: ## Run MatrixHub locally (web + API)
	make -j2 serve-web serve-api
