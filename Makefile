CARGO = cargo
CFLAG =
ENVS  = RUST_LOG=info RUST_BACKTRACE=1

all: run

run:
	$(ENVS) $(CARGO) run $(CFLAG) -- -i ./data/departments.ibd info

test:
	$(ENVS) $(CARGO) test -- --nocapture --show-output

t0:
	$(ENVS) $(CARGO) test tests::it_works -- --nocapture --show-output

doc:
	$(ENVS) $(CARGO) doc $(CFLAG)

browse:
	$(ENVS) $(CARGO) doc $(CFLAG) --no-deps --open

format:
	$(ENVS) $(CARGO) fmt $(CFLAG)

lint: format
	$(ENVS) $(CARGO) clippy $(CFLAG)

fix: format
	$(ENVS) $(CARGO) fix $(CFLAG) --allow-dirty --allow-staged

install:
	$(ENVS) $(CARGO) install $(CFLAG) --path .

clean:
	rm -rf target

# fake targets
.PHONY: all run test doc browse format lint fix install clean
