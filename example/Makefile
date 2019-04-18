CXX=g++
LDFLAGS= -lpthread -lrt -lprotobuf
CXXFLAGS=-O2 -std=c++11 -fno-builtin-memcmp -msse -msse4.2

.PHONY: clean all

#all: http_server mydispatch_srv myholy_srv myholy_srv_chandle myproto_cli \
#	redis_cli_test simple_http_server myredis_srv
all: parse_test 

ifndef PARSE_PATH
  $(warning Warning: missing pink path, using default)
  PARSE_PATH=$(CURDIR)/..
endif
PARSE_INCLUDE_DIR=$(PARSE_PATH)
PARSE_LIBRARY=$(PARSE_PATH)/lib

ifndef SLASH_PATH
  $(warning Warning: missing slash path, using default)
	SLASH_PATH=$(CURDIR)/../third
endif

SLASH_INCLUDE_DIR=$(SLASH_PATH)
SLASH_LIBRARY=$(SLASH_PATH)/slash/lib

CXXFLAGS+= -I$(PARSE_INCLUDE_DIR) -I$(SLASH_INCLUDE_DIR) 

LDFLAGS := $(DEP_LIBS) $(LDFLAGS) -L$(PARSE_LIBRARY) -L$(SLASH_LIBRARY) -lrdbparse -lslash

#http_server: http_server.cc
#	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

#https_server: https_server.cc
#	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

#mydispatch_srv: mydispatch_srv.cc myproto.pb.cc
#	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

#myholy_srv: myholy_srv.cc myproto.pb.cc
#	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

#myholy_srv_chandle: myholy_srv_chandle.cc myproto.pb.cc
#	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

#myproto_cli: myproto_cli.cc myproto.pb.cc
#	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

parse_test: parse_test.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS) 

#simple_http_server: simple_http_server.cc
#	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

#myredis_srv: myredis_srv.cc
#	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

#myproto.pb.cc: myproto.proto
#	protoc -I=. --cpp_out=. ./$^

clean:
	find . -name "*.[oda]" -exec rm -f {} \;
	rm -rf ./bg_thread ./http_server ./https_server ./mydispatch_srv ./myholy_srv \
					./myholy_srv_chandle ./myproto_cli ./redis_cli_test ./simple_http_server
