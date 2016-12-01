MODULE=wallet
SRCDIR=./src
DISTDIR=./dist
SERVER=$(DISTDIR)/$(MODULE)-server.js
PROCESSOR=$(DISTDIR)/$(MODULE)-processor.js
TMPSERVER=$(DISTDIR)/server.js
TMPPROCESSOR=$(DISTDIR)/processor.js
NPM=npm

all: $(SERVER) $(PROCESSOR)

$(TMPSERVER) $(TMPPROCESSOR): $(SRCDIR)/server.ts $(SRCDIR)/processor.ts
	tsc || rm $(TMPSERVER) $(TMPPROCESSOR)

$(SERVER): $(TMPSERVER)
	mv $< $@

$(PROCESSOR): $(TMPPROCESSOR)
	mv $< $@

$(SRCDIR)/server.ts: node_modules
$(SRCDIR)/processor.ts: node_modules

node_modules:
	$(NPM) install

clean:
	rm -rf $(DISTDIR)

.PHONY: all clean
