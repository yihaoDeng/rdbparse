CXX=g++
LDFLAGS= -lpthread -lrt
CXXFLAGS= -g -O0 -std=c++11 -fno-builtin-memcmp -msse -msse4.2

.PHONY: clean libslash


ifneq ($(DISABLE_UPDATE_SB), 1)
	$(info updating submodule)
dummy := $(shell (git submodule init && git submodule update))
endif

OBJECT = main

SLASH_PATH=./third
CXXFLAGS+= -I$(SLASH_PATH)

SLASH=$(SLASH_PATH)/slash/lib/libslash.a


OBJS = rdb.o util.o intset.o lzf_d.o ziplist.o zipmap.o

all: $(OBJECT) 
	@echo "Success, go, go, go..."

main: $(SLASH) $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(SLASH_PATH)/slash/lib/libslash.a $(LDFLAGS)

$(OBJS): %.o : %.cc 
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)	

$(SLASH):
	make -C $(SLASH_PATH)/slash/
clean:
	find . -name "*.[oda]*" -exec rm -f {} \;
	#rm -rf ./rdb 

libslash:
	cd .. && $(MAKE) static_lib
