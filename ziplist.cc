#include <list>
#include "ziplist.h"
#include "util.h"
#include "slash/include/slash_string.h"

using namespace slash;

bool ZiplistParser::Ziplist::GetVal(size_t *offset, std::string *str, bool *end) {
  char *p = entrys + *offset;     
  if (*p == kZiplistEnd) {
    *end = true;
    return true; 
  }
  p += (*p < kZiplistBegin) ? 1 : 5;
  *offset = p - entrys;

  uint8_t enc = static_cast<uint8_t>(*p);  
  enc &= kZipListStrMask;
  if (enc == kStrEnc6B || enc == kStrEnc14B || enc == kStrEnc32B) {
    return GetStrVal(offset, str);    
  }    
  char buf[16];
  int64_t val; 
  if (!GetIntVal(offset, &val)) { return false; }
  int len = ll2string(buf, sizeof(buf), val);
  str->assign(buf, len); 
  return true;
}
bool ZiplistParser::Ziplist::GetStrVal(size_t *offset, std::string *val) {
    char *p = entrys + *offset;
    uint8_t enc = static_cast<uint8_t>(*p);
    enc &= kZipListStrMask;  
    uint8_t skip = 0;
    uint32_t length = 0;
    if (enc == kStrEnc6B) {
        skip = 1;
        length = static_cast<uint32_t>(p[0]) 
            & static_cast<uint32_t>(~(kZipListStrMask)); 
    } else if (enc == kStrEnc14B) {
        skip = 2;
        length = ((static_cast<uint32_t>(p[0]) & ~kZipListStrMask) << 8)
            | static_cast<uint32_t>(p[1]);   
    } else if (enc == kStrEnc32B) {
        skip = 5;
        length = (static_cast<uint32_t>(p[1]) << 24)
            | (static_cast<uint32_t>(p[2]) << 16) 
            | (static_cast<uint32_t>(p[3]) << 8)
            | (static_cast<uint32_t>(p[4]));
    }
    p += skip;  
    p += length;
    val->assign(p, length);   
    *offset = p - entrys; 
    return true;
}
bool ZiplistParser::Ziplist::GetIntVal(size_t *offset, int64_t *val) {
    char *p = entrys + *offset; 
    if ((*p && kZipListStrMask) ^ kZipListStrMask) {
        return false;;
    } 
    uint8_t enc = static_cast<uint8_t>(*p) & ~(kZipListStrMask); 
    p++;
    if (enc == kIntEnc16) {
        int16_t v16;
        memcpy(&v16, p, sizeof(int16_t));
        *val = static_cast<int64_t>(v16); 
        memrev16ifbe(&v16);
        p += sizeof(int16_t);
    } else if (enc == kIntEnc64) {
        int64_t v64;
        memcpy(&v64, p, sizeof(int64_t));
        memrev64ifbe(&v64);
        *val = v64; 
        p += sizeof(int64_t);
    } else if (enc == kIntEnc24) {
        int32_t v32;
        memcpy(&v32, p, 3);
        *val = static_cast<int64_t>(v32); 
        memrev32ifbe(&v32);
        p += 3;
    } else if (enc == kIntEnc8) {
        int8_t v8; 
        memcpy(&v8, p, sizeof(int8_t));
        *val = static_cast<int64_t>(v8);
        p += sizeof(int8_t);
    } else if ((enc & kIntOther) == kIntOther){
        int8_t v8;
        v8 = static_cast<int8_t>(enc) & 0x0f;
        *val = static_cast<int64_t>(v8);
    }
    *offset = p - entrys; 
    return true;
}
Status ZiplistParser::GetListResult(std::list<std::string> *result) {
  bool ret, end = false;
  auto valid = [=] { return ret && !end; };
  std::string buf;
  while (valid()) {
    ret = ziplist_->GetVal(&offset_, &buf, &end);
    if (valid()) {
      result->push_back(buf);
    } else {
      break;
    }
  }
  return valid() ? Status::OK() : Status::Corruption("Parse error");
}

Status ZiplistParser::GetZsetOrHashResult(std::map<std::string, std::string> *result) {
  bool ret = true, end = false;
  auto valid = [=] { return ret && !end; };
  std::string key, value;
  while (valid()) {
      ret = ziplist_->GetVal(&offset_, &key, &end) 
            && ziplist_->GetVal(&offset_, &value, &end);   
      if (valid()) {
        result->insert({key, value}); 
      } else {
        break;
      }
  } 
  return valid() ? Status::OK() : Status::Corruption("Parse error"); 
}

