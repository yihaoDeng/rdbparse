#ifndef __ZIPMAP__
#define __ZIPMAP__
#include <map>
#include "util.h"
#include "slash/include/slash_status.h"
using namespace slash;

class ZipmapParser {
  public:
    ZipmapParser(void  *buf); 
    enum Mark {
      kZipmapEnd = 0xff, 
      kZipmapBiglen = 0xfd,
    };
    struct Zipmap {
       char entrys[0];         
       bool GetValue(bool skip_free, size_t *offset, std::string *value);
       bool GetKV(size_t *offset, std::string *key, std::string *value, bool *end); 
       bool IsEnd(size_t *offset);
       uint32_t GetEntryLenSize(char *entry); 
       uint32_t GetEntryStrLen(uint8_t len_size, char *entry); 
    };
    
    Status GetResult(std::map<std::string, std::string> *result);
  private:
    Zipmap *handle_;
    size_t offset_;     
};

#endif
