#include <list>
#include "ziplist.h"
#include "util.h"
#include "slash/include/slash_string.h"

using namespace slash;

bool ZiplistParser::Ziplist::Get(size_t *offset, std::string *str, bool *end) {
  char *p = entrys + *offset;     
  if (static_cast<uint8_t>(*p) == kZiplistEnd) {
    *end = true;
    return true; 
  }
  p += (*p < kZiplistBegin) ? 1 : 5;
  *offset = p - entrys;

  uint8_t enc = static_cast<uint8_t>(*p) & kZipListStrMask; 
  if (enc == kStrEnc6B || enc == kStrEnc14B || enc == kStrEnc32B) {
    return GetStr(offset, str);    
  }    

  int64_t val; 
  if (!GetInt(offset, &val)) { 
    return false; 
  }

  char buf[16];
  int len = ll2string(buf, sizeof(buf), val);
  str->assign(buf, len); 
  return true;
}
bool ZiplistParser::Ziplist::GetStr(size_t *offset, std::string *val) {
  char *p = entrys + *offset;
  uint8_t skip = 0, enc = static_cast<uint8_t>(*p) & kZipListStrMask;
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
  //p += (skip + length);  
  val->assign(p, length);   
   
  *offset = p - entrys + length; 
  return true;
}
bool ZiplistParser::Ziplist::GetInt(size_t *offset, int64_t *val) {
  char *p = entrys + *offset; 
  uint8_t enc = static_cast<uint8_t>(*p); 
  p++;
  if (enc == kIntEnc16) {
    int16_t v16;
    memcpy(&v16, p, sizeof(int16_t));
    memrev16ifbe(&v16);
    *val = static_cast<int64_t>(v16); 
    p += sizeof(int16_t);
  } else if (enc == kIntEnc32) {
    int32_t v32;
    memcpy(&v32, p, sizeof(int32_t));
    memrev32ifbe(&v32);
    *val = v32; 
    p += sizeof(int32_t);
  } else if (enc == kIntEnc64) {
    int64_t v64;
    memcpy(&v64, p, sizeof(int64_t));
    memrev64ifbe(&v64);
    *val = v64; 
    p += sizeof(int64_t);
  } else if (enc == kIntEnc24) {
    int32_t v32;
    memcpy(&v32, p, 3);
    memrev32ifbe(&v32);
    *val = static_cast<int64_t>(v32); 
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
ZiplistParser::ZiplistParser(void *buf)
  : handle_(reinterpret_cast<Ziplist *>(buf)), 
    offset_(0) {
  }
Status ZiplistParser::GetList(std::list<std::string> *result) {
  bool ret = true, end = false;
  auto valid = [&] { return ret && !end; };
  while (valid()) {
    std::string buf;
    ret = handle_->Get(&offset_, &buf, &end);
    if (!valid()) { 
      break; 
    }
    result->push_back(buf);
  }
  return ret ? Status::OK() : Status::Corruption("Parse error");
}

Status ZiplistParser::GetZsetOrHash(std::map<std::string, std::string> *result) {
  bool ret = true, end = false;
  auto valid = [&] { return ret && !end; };
  while (valid()) {
    std::string key, value;
    ret = handle_->Get(&offset_, &key, &end) 
      && handle_->Get(&offset_, &value, &end);   
    if (!valid()) { 
      break;
    }
    result->insert({key, value}); 
  } 
  return ret ? Status::OK() : Status::Corruption("Parse error"); 
}

