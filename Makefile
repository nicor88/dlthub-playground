.PHONY: init
init: clean init-venv init-env

.PHONY: init-venv
init-venv:
	uv venv --python=3.12;
	uv sync;

.PHONY: clean
clean:
	rm -rf .venv;

.PHONY: init-env
init-env:
	@if [ -f .env ]; then \
		touch .env; \
	else \
		cp .env.template .env; \
	fi
