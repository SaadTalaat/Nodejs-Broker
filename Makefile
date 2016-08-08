PATH := ./node_modules/.bin/:${PATH}

.PHONY: clean build test install publish


clean:
	rm -rf lib/


build:
	coffee -o lib/ -c src/

test:
	npm test


publish: build
	npm publish
