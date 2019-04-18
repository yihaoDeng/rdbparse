#ifndef __ZIPLIST_H__
#define __ZIPLIST_H__
#include <list>
#include <map>
#include "slash/include/env.h" 

using namespace slash;

enum ZiplistFlag {
  kZiplistBegin = 254,
  kZiplistEnd = 0xff,
  kZipListStrMask = 0xc0,
  kZipListIntMask = 0x30                 
};
enum ZiplistStrEncType {
  kStrEnc6B = 0,
  kStrEnc14B = 1 << 6,
  kStrEnc32B = 1 << 7,
};

enum ZiplistIntEncType {
  kIntEnc16 = 0xc0, 
  kIntEnc32 = 0xd0,
  kIntEnc64 = 0xe0, 
  kIntEnc24 = 0xf0,
  kIntEnc8 = 0xfe, 
  kIntOther = 0xf0
};
struct Ziplist;

class ZiplistParser {
  public:
    ZiplistParser(void *buf);
   
    struct Ziplist {
      uint32_t bytes;
      uint32_t ztail;
      uint16_t len;
      char entrys[0];
      bool Get(size_t *offset, std::string *buf, bool *end);

      bool GetInt(size_t *offset, int64_t *v);
      bool GetStr(size_t *offset, std::string *v);
    };

    Status GetList(std::list<std::string> *result);
    Status GetZsetOrHash(std::map<std::string, std::string> *result); 
  private:
    Ziplist *handle_; 
    size_t offset_;
};

#endif
