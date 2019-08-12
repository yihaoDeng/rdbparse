#include <list>
#include "ziplist.h"
#include "util.h"

namespace parser {


bool ZiplistParser::Ziplist::Get(size_t *offset, std::string *str, bool *end) {
  char *p = entrys + *offset;     
  if (static_cast<uint8_t>(*p) == kZiplistEnd) {
    *end = true;
    return true; 
  }
  p += (static_cast<uint8_t>(*p) < kZiplistBegin) ? 1 : 5;
  *offset = p - entrys;

  uint8_t enc = static_cast<uint8_t>(*p) & kZipListStrMask; 
  if (enc == kStrEnc6B || enc == kStrEnc14B || enc == kStrEnc32B) {
    return GetStr(offset, str);    
  }    

  int64_t val; 
  if (!GetInt(offset, &val)) { 
    return false; 
  }
  str->assign(std::to_string(val)); 
  return true;
}
bool ZiplistParser::Ziplist::GetStr(size_t *offset, std::string *val) {
  char *p = entrys + *offset;
  uint8_t skip = 0, enc = static_cast<uint8_t>(*p) & kZipListStrMask;
  uint32_t length = 0;
  if (enc == kStrEnc6B) {
    skip = 1;
    length = static_cast<uint8_t>(p[0]) 
      & static_cast<uint32_t>(~(kZipListStrMask)); 
  } else if (enc == kStrEnc14B) {
    skip = 2;
    length = ((static_cast<uint8_t>(p[0]) & ~kZipListStrMask) << 8)
      | static_cast<uint8_t>(p[1]);   
  } else if (enc == kStrEnc32B) {
    skip = 5;
    length = (static_cast<uint8_t>(p[1]) << 24)
      | (static_cast<uint8_t>(p[2]) << 16) 
      | (static_cast<uint8_t>(p[3]) << 8)
      | (static_cast<uint8_t>(p[4]));
  }
  p += skip;
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
    memcpy(&v16, p, 2);
    *val = static_cast<int64_t>(v16); 
    p += 2;
  } else if (enc == kIntEnc32) {
    int32_t v32;
    memcpy(&v32, p, 4);
    *val = v32; 
    p += 4;
  } else if (enc == kIntEnc64) {
    int64_t v64;
    memcpy(&v64, p, 8);
    *val = v64; 
    p += 8;
  } else if (enc == kIntEnc24) {
    char buf[4] = {0};
    memcpy(buf + 1, p, 3); 
    int32_t v32;
    memcpy(&v32, buf, 4);
    *val = static_cast<int64_t>(v32 >> 8); 
    p += 3;
  } else if (enc == kIntEnc8) {
    int8_t v8; 
    memcpy(&v8, p, 1);
    *val = static_cast<int64_t>(v8);
    p += 1;
  } else if (enc >= 241 && enc <= 253){
    int8_t v8 = static_cast<uint8_t>(enc) - 241; 
    *val = static_cast<int64_t>(v8);
  } else {
    return false;
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
}
