CXX=g++
LDFLAGS= -lpthread -lrt -lprotobuf
CXXFLAGS= -g -std=c++11 -fno-builtin-memcmp -msse -msse4.2

PROFILING_FLAGS=-pg

.PHONY: clean all

DEBUG_LEVEL?=0

ifeq ($(DEBUG_LEVEL),0)
DISABLE_WARNING_AS_ERROR=1
OPT += -O2 -fno-omit-frame-pointer -DNDEBUG
else
OPT += -O0 -D__XDEBUG__ $(PROFILING_FLAGS)
DEBUG_SUFFIX = "_debug"
endif

#all: http_server mydispatch_srv myholy_srv myholy_srv_chandle myproto_cli \
#	redis_cli_test simple_http_server myredis_srv
all: parse_test$(DEBUG_SUFFIX) 

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

CXXFLAGS+= $(OPT) -I$(PARSE_INCLUDE_DIR) -I$(SLASH_INCLUDE_DIR) 

LDFLAGS := $(DEP_LIBS) $(LDFLAGS) -L$(PARSE_LIBRARY) -L$(SLASH_LIBRARY) -lrdbparse$(DEBUG_SUFFIX) -lslash$(DEBUG_SUFFIX)

parse_test$(DEBUG_SUFFIX): parse_test.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS) 

#simple_http_server: simple_http_server.cc
#	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

#myredis_srv: myredis_srv.cc
#	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

#myproto.pb.cc: myproto.proto
#	protoc -I=. --cpp_out=. ./$^

clean:
	find . -name "*.[oda]" -exec rm -f {} \;
	rm -rf ./parse_test$(DEBUG_SUFFIX) 
