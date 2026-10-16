

.PHONY: run
run:
	go run ./cmd/hfd

.PHONY: update-hf-api-status
update-hf-api-status:
	go run ./hack/update-hf-api-status

BENCH ?= .
BENCHTIME ?= 1s

# Engine benchmarks (native git vs go-git). HFD_BENCH_SCALE scales the
# fixtures, HFD_BENCH_PARALLEL the parallel-clone row; B/op is meaningful
# for go-git only (native work happens in child processes). -bench matches
# a row name level by level, split on /, e.g.
# make bench BENCH='BenchmarkLsRemote/engine=native/transport=http' or
# make bench BENCH='BenchmarkClone/.*/.*/.*/shape=many-objects'.
.PHONY: bench
bench:
	go test -run '^$$' -bench '$(BENCH)' -benchmem -benchtime $(BENCHTIME) -count=1 -timeout 90m ./test/e2e/
