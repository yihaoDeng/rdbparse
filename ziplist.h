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
  kIntEnc16 = 0, 
  kIntEnc32 = 1 << 4,
  kIntEnc64 = 1 << 5, 
  kIntEnc24 = (1 << 4) | (1 << 5),
  kIntEnc8 = 0x3e, 
  kIntOther = 0x30
};
struct Ziplist;

class ZiplistParser {
  public:
    ZiplistParser(void *buf) {
      ziplist_ = reinterpret_cast<Ziplist *>(buf); 
      offset_ = 0; 
    }
    struct Ziplist {
      uint32_t bytes;
      uint32_t ztail;
      uint16_t len;
      char entrys[0];
      bool GetVal(size_t *offset, std::string *buf, bool *end);

      bool GetIntVal(size_t *offset, int64_t *v);
      bool GetStrVal(size_t *offset, std::string *v);
    };
    Status GetListResult(std::list<std::string> *result);
    Status GetZsetOrHashResult(std::map<std::string, std::string> *result); 
  private:
    Ziplist *ziplist_; 
    size_t offset_;
};

#endif
