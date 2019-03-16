#include <arpa/inet.h>
#include <iostream>
#include <list>
#include <set>
#include "slash/include/slash_string.h"
#include "rdb.h"
#include "util.h"
#include "intset.h"
#include "lzf.h"
#include "ziplist.h"
#include "zipmap.h"

using namespace slash;

struct RdbParser::Arena {
  Arena() : buf(NULL), buf_size(0) {}
  ~Arena() {
    delete [] buf; 
  }
  void EnlargeBufferIfNeed(size_t len) {
    if (len <= sizeof(space)) {
      key = space;
      key_size = len;
      return;
    }
    if (len > buf_size) {
      delete [] buf;
      buf = new char[len];
      buf_size = len;
    }
    key = buf;
    key_size = len;  
    return;
  }   
  char *AllocateBuffer(size_t len) {
    EnlargeBufferIfNeed(len);
    return key;
  }
  char *Value() const {
    return key;
  }
  size_t ValueSize() const {
    return key_size;
  }
  char *buf; 
  size_t buf_size;
  char *key;
  size_t key_size;
  char space[32];
};

RdbParser::RdbParser(const std::string &path):
  path_(path), version_(kMagicString.size()), 
  result_(new ParsedResult), valid_(true) {
  }

RdbParser::~RdbParser() {
  delete result_;
}

Status RdbParser::Init() {
  Status s = NewSequentialFile(path_, &sequence_file_);  
  if (!s.ok()) { return s; }

  char buf[16];
  Slice result;
  s = ReadAndChecksum(9, &result, buf);  
  if (!s.ok() || kMagicString != result.ToString().substr(0, kMagicString.size())) {
    return Status::Incomplete("unsupport rdb head magic");
  }
  result.remove_prefix(kMagicString.size());  
  long version = 0; 
  if (!string2l(result.data(), result.size(), &version)) {
    return Status::Corruption("unsupport rdb version");
  }
  version_ = static_cast<int>(version);
  return Status::OK();
}
ParsedResult* RdbParser::Value() {
  return result_;
}
Status RdbParser::ReadAndChecksum(uint64_t len, Slice *result, char *scratch) {
  Status s = sequence_file_->Read(len, result, scratch); 
  if (s.ok() && version_ >= 5) {
    const unsigned char *p1 = (const unsigned char *)scratch; 
    check_sum_ = crc64(check_sum_, p1, len); 
  } else {
    return Status::Incomplete("Incomplete data");
  }
  return s; 
}
Status RdbParser::LoadExpiretime(uint8_t type, int *expire_time) {
  char buf[8];
  Status s;
  Slice result;
  if (type == kRdbExpireMs) {
    uint64_t t64;  
    s = ReadAndChecksum(8, &result, buf); 
    memcpy(&t64, buf, 8);
    *expire_time = static_cast<int>(t64/1000); 
  } else {
    uint32_t t32;
    s = ReadAndChecksum(4, &result, buf); 
    memcpy(&t32, buf, 4);
    *expire_time = static_cast<int>(t32);
  }
  return s; 
}
Status RdbParser::LoadEntryType(uint8_t *type) {
  char buf[1];
  Slice result;
  Status s = ReadAndChecksum(1, &result, buf); 
  if (!s.ok()) { return s; }
  *type = static_cast<uint8_t>(buf[0]);    
  return s;
}
Status RdbParser::LoadEntryKey(std::string *result) {
  return LoadString(result); 
}

Status RdbParser::LoadEntryDBNum(uint8_t *db_num) {
  char buf[1];
  Slice result;
  Status s = ReadAndChecksum(1, &result, buf); 
  if (!s.ok()) { return s; } 
  *db_num = static_cast<uint8_t>(buf[0]);
  return s;
}
Status RdbParser::LoadIntVal(uint32_t type, std::string *result) {
  char buf[8];
  Slice slice_buf; 
  Status s;
  int32_t val;
  if (type == kRdbEncInt8) {
    s = ReadAndChecksum(1, &slice_buf, buf);
    if (!s.ok()) { return s; }
    val = static_cast<int8_t>(buf[0]);  
  } else if (type == kRdbEncInt16) {
    s = ReadAndChecksum(2, &slice_buf, buf);
    if (!s.ok()) { return s; }
    val = static_cast<int8_t>(buf[0]) | (static_cast<int8_t>(buf[1]) << 8);
  } else if (type == kRdbEncInt32) {
    s = ReadAndChecksum(4, &slice_buf, buf);
    if (!s.ok()) { return s; }
    val = static_cast<int8_t>(buf[0]) | (static_cast<int8_t>(buf[1]) << 8) 
      | (static_cast<int8_t>(buf[2]) << 16) | (static_cast<int8_t>(buf[3]) << 24);
  } else {
    return Status::Corruption("no supported type");
  }
  ll2string(buf, sizeof(buf), val);   
  result->assign(buf);
  return s; 
}

Status RdbParser::LoadEncLzf(std::string *result) {
  uint32_t raw_len, compress_len;    
  Status s = LoadFieldLen(&compress_len, NULL);
  if (!s.ok()) { return s; }
  s = LoadFieldLen(&raw_len, NULL);
  if (!s.ok()) { return s; }

  char *compress_buf = new char[compress_len];
  char *raw_buf = new char[raw_len]; 
  if (!compress_buf || !raw_buf) { 
    return Status::Corruption("no enough memory to alloc"); 
  }
  Slice compress_slice; 
  bool ret = ReadAndChecksum(compress_len, &compress_slice, compress_buf).ok() 
           && (0 != DecompressLzf(compress_buf, compress_len, raw_buf, raw_len));
  if (ret) {
    result->assign(raw_buf, raw_len);
  }
  delete [] compress_buf;
  delete [] raw_buf;
  return ret ? Status::OK() : Status::Corruption("Parse error"); 
}
void RdbParser::ResetResult() {
  result_->db_num = 0;
  result_->expire_time = -1;
  result_->key.clear(); 
  result_->kv_value.clear(); 
  result_->set_value.clear();
  result_->map_value.clear();
  result_->list_value.clear();
}
Status RdbParser::LoadListZiplist(std::list<std::string> *value) {
  std::string buf;
  Status s = LoadString(&buf); 
  if (!s.ok()) { return s; }
  ZiplistParser ziplist_parser((void *)(buf.c_str()));  
  return ziplist_parser.GetListResult(value);
}
Status RdbParser::LoadZsetOrHashZiplist(std::map<std::string, std::string> *result) {
  std::string buf;
  Status s = LoadString(&buf);
  if (!s.ok()) { return s; }
  ZiplistParser ziplist_parser((void *)buf.c_str());
  return ziplist_parser.GetZsetOrHashResult(result);
}
Status RdbParser::LoadZipmap(std::map<std::string, std::string> *result) {
  std::string buf;
  Status s = LoadString(&buf);
  if (!s.ok()) { return s; }
  ZipmapParser zipmap_parser((void *)(buf.c_str()));
  return zipmap_parser.GetResult(result);
}
Status RdbParser::LoadListOrSet(std::list<std::string> *result) {
  uint32_t i = 0, field_size;   
  Status s = LoadFieldLen(&field_size, NULL);  
  if (!s.ok()) { return s; }
  std::string val;
  for (; i < field_size; i++) {
    bool ret = LoadString(&val).ok();   
    if (!ret) { break; }
    result->push_back(val);
  }
  return i == field_size ? Status::OK() : Status::Corruption("Parse error");
} 
Status RdbParser::LoadHashOrZset(std::map<std::string, std::string> *result) {
  uint32_t i = 0, field_size;   
  Status s = LoadFieldLen(&field_size, NULL);  
  if (!s.ok()) { return s; }
  std::string key, value;
  for (; i < field_size; i++) {
    bool ret = LoadString(&key).ok() && LoadString(&value).ok();
    if (!ret) { break; }
    result->insert({key, value});
  }
  return i == field_size ? Status::OK() : Status::Corruption("Parse error");
}
Status RdbParser::LoadString(std::string *result) {
  uint32_t len;
  bool is_encoded = false;
  Status s = LoadFieldLen(&len, &is_encoded);
  if (!s.ok()) { return s; } 
  if (is_encoded) {
    switch (len) {
      case kRdbEncInt8: 
      case kRdbEncInt16:
      case kRdbEncInt32:
        return LoadIntVal(len, result);      
      case kRdbEncLzf:   
        return LoadEncLzf(result);
      default:
        return Status::Corruption("");
    }  
  }
  Slice buf_slice;
  char *buf = new char[len];
  s = ReadAndChecksum(len, &buf_slice, buf);  
  if (s.ok()) result->assign(buf_slice.data(), buf_slice.size());
  delete [] buf;
  return s;
}

Status RdbParser::LoadFieldLen(uint32_t *length, bool *is_encoded) {
  char buf[8];      
  Slice result;
  Status s = ReadAndChecksum(1, &result, buf);  
  if (!s.ok()) { 
    *length = kRdbLenErr;
    return s; 
  }
  uint32_t len;
  uint8_t type = (buf[0] & 0xc0) >> 6; 
  if (type == kRdb6B) {
    len = buf[0] & 0x3f;
  } else if (type == kRdb14B) {
    s = ReadAndChecksum(1, &result, buf + 1); 
    if (!s.ok()) { return s; }
    len = ((buf[0] && 0x3f) << 8) | buf[1];   
  } else if (type == kRdb32B) {
    s = ReadAndChecksum(4, &result, buf); 
    memcpy(&len, buf, 4);  
    if (!s.ok()) { return s; }
    len = ntohl(len);
  } else {
    if (is_encoded) { *is_encoded = true; }   
    len = buf[0] & 0x3f;
  }
  *length = len;
  return s;
}
Status RdbParser::LoadEntryValue(uint8_t type) {
  Status s;
  switch (type) {
    case kRdbString:  
      s = LoadString(&(result_->kv_value)); 
      if (!s.ok()) { return s; }         
      break; 
    case kRdbIntset:
      s = LoadIntset(&(result_->set_value));               
      if (!s.ok()) { return s; }              
    case kRdbListZiplist: 
      s = LoadListZiplist(&(result_->list_value));
      if (!s.ok()) { return s; }  
      break;
    case kRdbHashZipMap:
      s = LoadZipmap(&(result_->map_value));
      if (!s.ok()) { return s; }
      break;
    case kRdbZsetZiplist:               
    case kRdbHashZiplist:
      s = LoadZsetOrHashZiplist(&(result_->map_value));
      if (!s.ok()) { return s; }
      break;
    case kRdbList:
    case kRdbSet:                
      s = LoadListOrSet(&(result_->list_value));
      if (s.ok()) { return s; }
      break;
    case kRdbHash:
    case kRdbZset:
      s = LoadHashOrZset(&(result_->map_value));
      break;
    default: 
      return Status::Corruption("Invalid value type");
  }
  return Status::OK();
}
Status RdbParser::LoadIntset(std::set<std::string> *result) {
  std::string value;
  Status s = LoadString(&value);    
  Intset *int_set = reinterpret_cast<Intset *>((void *)(value.data())); 
  size_t i = 0;
  for (; i < int_set->length; i++) {
    int64_t v64;
    char buf[16];
    if (!int_set->Get(i, &v64).ok()) { break; }
    int size = ll2string(buf, sizeof(buf), v64);
    if (size <= 0) { break; } 
    result->emplace(buf, size);
  } 
  return i == int_set->length ? Status::OK() : Status::Corruption("Parse error");
}
bool RdbParser::Valid() {
  return valid_;
}
Status RdbParser::Next() {
  // RdbEntryType type;
  // 1. load expire time
  // 2. load db num;
  // 3. load key 
  // 4. load value
  uint8_t type;
  while (1) {
    Status s = LoadEntryType(&type);
    if (!s.ok()) { return s; }
    if (type == kRdbEof) {
      valid_ = false;
      return Status::OK(); 
    }
    if (type == kRdbSelectDb) {
      s = LoadEntryDBNum(&(result_->db_num));
      if (!s.ok()) { return s; } 
      continue;
    } 

    if (type == kRdbExpireMs || type == kRdbExpireSec) {
      s = LoadExpiretime(type, &(result_->expire_time));
      if (!s.ok()) { return s; }
      continue;
    }  
    s = LoadEntryKey(&(result_->key));        
    if (!s.ok()) { return s; } 
    result_->type = GetTypeName(ValueType(type));
    s = LoadEntryValue(type);
    return s;
  }
} 

std::string RdbParser::GetTypeName(ValueType type) {
  static std::unordered_map<ValueType, std::string, std::hash<int>> type_map{
    { kRdbString, "string"}, { kRdbList, "list"},
      { kRdbSet, "set"}, { kRdbHashZipMap,"hash"},
      { kRdbZsetZiplist, "zset"}, { kRdbHashZiplist, "hash"},
      { kRdbListZiplist,"list"}, { kRdbIntset, "set"},
      { kRdbHash,"hash"}, { kRdbZsetZiplist,"zset"}
  };
  auto it = type_map.find(type);  
  return it != type_map.end() ? it->second : ""; 
}
int main() {
  std::string rdb_path = "rdb/dump2.8.rdb";
  RdbParser parse(rdb_path); 
  Status s = parse.Init();
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    return 1;
  }
  while (parse.Valid()) {
    s = parse.Next(); 
    if (s.ok() && parse.Valid()) {
         
    } else if (s.ok()) {
      std::cout << "end file" << std::endl;
      break;
    } else {
      std::cout << "parse error: " << s.ToString()  << std::endl;
      break;
    } 
  }
  
  return 1;
}
