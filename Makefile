DISTDIR=./dist
SRCDIR=./src
SERVER=$(DISTDIR)/server.js
NPM=cnpm

all: $(SERVER)

$(SERVER): $(SRCDIR)/server.ts
	tsc
	
$(SRCDIR)/server.ts: node_modules typings

node_modules:
	$(NPM) install

typings:
	typings install

clean:
	rm -rf $(DISTDIR)

.PHONY: all clean
