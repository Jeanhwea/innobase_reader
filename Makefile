CARGO  = cargo
CFLAG  =
DOCDIR = $(HOME)/code/github/read0code/read0code.github.io/pub/ibr

all: run

run:
	$(CARGO) run $(CFLAG) -- ./data/departments.ibd info

test:
	$(CARGO) test -- --nocapture --show-output

t0:
	$(CARGO) test undo_tests::test_read_undo_record -- --nocapture --show-output

doc:
	$(CARGO) doc $(CFLAG)

browse:
	$(CARGO) doc $(CFLAG) --no-deps --open

debug:
	rust-gdb --args ./target/debug/ibr $(A)

format:
	git add src
	rustup run nightly $(CARGO) fmt $(CFLAG)

lint: format
	$(CARGO) clippy $(CFLAG)

fix: format
	$(CARGO) fix $(CFLAG) --allow-dirty --allow-staged

install:
	$(CARGO) install $(CFLAG) --path .

install-offline:
	$(CARGO) install $(CFLAG) --offline --path .

release: release-linux

# rustup target add x86_64-unknown-linux-musl
release-linux:
	$(CARGO) build --release $(CFLAG) --target=x86_64-unknown-linux-musl

release-win:
	$(CARGO) build --release $(CFLAG) --target=x86_64-pc-windows-gnu

publish-doc: clean
	@if test -d $(DOCDIR) ; then \
	  $(CARGO) doc $(CFLAG) -q --no-deps && \
	  rm -rf $(DOCDIR) && \
	  cp -rf ./target/doc $(DOCDIR) && \
	  echo "save to $(DOCDIR)"; \
	fi;

clean:
	@rm -rf target

# fake targets
.PHONY: all run test doc browse format lint fix install clean
