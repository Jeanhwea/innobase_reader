CARGO = cargo
CFLAG =

all: run

run:
	$(CARGO) run $(CFLAG) -- ./data/departments.ibd info

test:
	$(CARGO) test -- --nocapture --show-output

t0:
	$(CARGO) test app_tests::it_works -- --nocapture --show-output

doc:
	$(CARGO) doc $(CFLAG)

browse:
	$(CARGO) doc $(CFLAG) --no-deps --open

format:
	$(CARGO) fmt $(CFLAG)

lint: format
	$(CARGO) clippy $(CFLAG)

fix: format
	$(CARGO) fix $(CFLAG) --allow-dirty --allow-staged

install:
	$(CARGO) install $(CFLAG) --path .

release: release-linux release-win

# rustup target add x86_64-unknown-linux-musl
release-linux:
	$(CARGO) build --release $(CFLAG) --target=x86_64-unknown-linux-musl

release-win:
	$(CARGO) build --release $(CFLAG) --target=x86_64-pc-windows-gnu

clean:
	rm -rf target

# fake targets
.PHONY: all run test doc browse format lint fix install clean
