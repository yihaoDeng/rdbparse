#include "zipmap.h"
#include "util.h"
#include "slash/include/slash_status.h"
using namespace slash;

bool ZipmapParser::Zipmap::GetValue(bool skip_free, size_t *offset, std::string *value) {
  uint8_t skip_step = skip_free ? 0 : 1;  
  char *p = entrys + *offset;    
  uint32_t len_size = GetEntryLenSize(p);
  uint32_t str_len = GetEntryStrLen(len_size, p);
  value->append(p + len_size + skip_step, str_len); 
  p += (len_size + str_len + skip_step);
  *offset = p - entrys;
  return true;
}
bool ZipmapParser::Zipmap::GetKV(size_t *offset, std::string *key, std::string *value, bool *end) {
  if (IsEnd(offset)) {
    *end = true;
    return true;
  }
  GetValue(false, offset, key);
  GetValue(true, offset, value);
  return true;
}
bool ZipmapParser::Zipmap::IsEnd(size_t *offset) {
  char *p = entrys + *offset;    
  if (static_cast<uint8_t>(*p) == kZipmapEnd) { return true; }
  return false;
}
uint32_t ZipmapParser::Zipmap::GetEntryLenSize(char *entry) {
  int8_t flag = static_cast<int8_t>(entry[0]);
  return flag < kZipmapBiglen ? 1 : 5; 
}
uint32_t ZipmapParser::Zipmap::GetEntryStrLen(uint8_t len_size, char *entry) {
  uint32_t str_len = 0;
  if (len_size == 1) {
    str_len = static_cast<uint32_t>(entry[0]);
  } else if (len_size == 5) {
    memcpy(&str_len, entry + 1, 4); 
    memrev32ifbe(str_len); 
  }
  return str_len; 
}
ZipmapParser::ZipmapParser(void *buf)
  : handle_(reinterpret_cast<ZipmapParser::Zipmap *>(buf)), 
    offset_(0){
}

Status ZipmapParser::GetResult(std::map<std::string, std::string> *result) {
  bool ret = true, end = false;
  auto valid = [=] { return ret && !end; };
  offset_ += 1;
  while (valid()) {
    std::string key, value;
    ret = handle_->GetKV(&offset_, &key, &value, &end);
    if (!valid()) { break; }
    result->insert({key, value});
  }
  return valid() ? Status::OK() : Status::Corruption("Parse error");
}
