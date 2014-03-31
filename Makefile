
all: xref

xref:
	@./rebar clean
	@./rebar get-deps
	@./rebar compile
	@./rebar xref skip_deps=true
