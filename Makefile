CXX=g++
LDFLAGS= -lpthread -lrt
CXXFLAGS= -O2 -std=c++11 -fno-builtin-memcmp -msse -msse4.2

.PHONY: clean libslash

OBJECT = main

SLASH_PATH=./third
CXXFLAGS+= -I$(SLASH_PATH)

OBJS = rdb.o util.o intset.o lzf_d.o ziplist.o zipmap.o

all: $(OBJECT) 
	@echo "Success, go, go, go..."

main: $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(SLASH_PATH)/slash/lib/libslash.a $(LDFLAGS)

$(OBJS): %.o : %.cc 
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)	

clean:
	find . -name "*.[oda]*" -exec rm -f {} \;
	rm -rf ./rdb 

libslash:
	cd .. && $(MAKE) static_lib
