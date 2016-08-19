DISTDIR=./dist
SRCDIR=./src
SERVER=$(DISTDIR)/server.js
PROCESSOR=$(DISTDIR)/processor.js
NPM=cnpm

all: $(SERVER) $(PROCESSOR)

$(SERVER) $(PROCESSOR): $(SRCDIR)/server.ts $(SRCDIR)/processor.ts
	tsc || rm $(SERVER) $(PROCESSOR)
	
$(SRCDIR)/server.ts: node_modules typings
$(SRCDIR)/processor.ts: node_modules typings

node_modules:
	$(NPM) install

typings:
	typings install

clean:
	rm -rf $(DISTDIR)

.PHONY: all clean
