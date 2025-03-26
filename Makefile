REBAR ?= rebar3

.PHONY: node1 node2 node3 node4 node5 node

all: compile

clean:
	$(REBAR) clean

test: eunit cover
	${REBAR} as test ct

eunit:
	${REBAR} as test eunit

cover:
	${REBAR} cover

node1:
	${REBAR} as node1 release
	ERL_NODE_NAME=node1@127.0.0.1 \
	PARTISAN_PEER_PORT=10100 \
	DIST_PEER_PORT=20100 \
	RELX_REPLACE_OS_VARS=true \
	_build/node1/rel/erleans/bin/erleans console

node2:
	${REBAR} as node2 release
	ERL_NODE_NAME=node2@127.0.0.1 \
	PARTISAN_PEER_PORT=10200 \
	DIST_PEER_PORT=20200 \
	RELX_REPLACE_OS_VARS=true \
	_build/node2/rel/erleans/bin/erleans console

node3:
	${REBAR} as node3 release
	ERL_NODE_NAME=node3@127.0.0.1 \
	PARTISAN_PEER_PORT=10300 \
	DIST_PEER_PORT=20300 \
	RELX_REPLACE_OS_VARS=true \
	_build/node3/rel/erleans/bin/erleans console

node4:
	${REBAR} as node5 release
	ERL_NODE_NAME=node4@127.0.0.1 \
	PARTISAN_PEER_PORT=10400 \
	DIST_PEER_PORT=20400 \
	RELX_REPLACE_OS_VARS=true \
	_build/node3/rel/erleans/bin/erleans console

node5:
	${REBAR} as node5 release
	ERL_NODE_NAME=node5@127.0.0.1 \
	PARTISAN_PEER_PORT=10500 \
	DIST_PEER_PORT=20500 \
	RELX_REPLACE_OS_VARS=true \
	_build/node3/rel/erleans/bin/erleans console