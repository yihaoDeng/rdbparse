

#include <arpa/inet.h>
#include <iostream>
#include <list>
#include <set>

#include "rdbparse_impl.h"
#include "include/rdbparse.h"
#include "util.h"
#include "intset.h"
#include "lzf.h"
#include "ziplist.h"
#include "zipmap.h"

namespace parser {

void ParsedResult::Debug() {
  static std::set<std::string> type_set{"set", "string", "zset", "hash", "list"};
  if (!type_set.count(this->type)) {
    return;
  }
  printf("db_num:%d, expire_time: %d, type: %s, key: %s,", this->db_num, this->expire_time, this->type.c_str(), this->key.c_str()); 
  if (this->type == "string") {
    printf("value: %s\n", this->kv_value.c_str());
  } else if (this->type == "hash") {
    printf("value:["); 
    auto &map_value = this->map_value;
    for(auto it = map_value.begin(); it != map_value.end();) {
      printf("%s -> %s", it->first.c_str(), it->second.c_str()); 
      it++; 
      if (it != map_value.end()) {
        printf(", ");
      }
    }
    printf("]\n");
  } else if (this->type == "list") {
    printf("value: []\n");  
  } else if (this->type == "set") {
    printf("value:["); 
    auto &set_value = this->set_value;
    if (!set_value.empty()) {
      for (auto it = set_value.begin(); it != set_value.end();) {
        printf("%s", (*it).c_str());
        it++;
        if (it != set_value.end()) {
          printf(", ");
        }
      }
    } else {
      auto& list_value = this->list_value; 
      for (auto it = list_value.begin(); it != list_value.end();) {
        printf("%s", (*it).c_str());
        it++;
        if (it != list_value.end()) {
          printf(", ");
        }
      }
    }
    printf("]\n"); 
  }
}

struct RdbParseImpl::Arena {
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

RdbParseImpl::RdbParseImpl(const std::string &path):
  path_(path), version_(kMagicString.size()), 
  result_(new ParsedResult), valid_(true) {
  }

RdbParseImpl::~RdbParseImpl() {
  delete result_;
}

Status RdbParseImpl::Init() {
  Status s = NewSequentialFile(path_, &sequence_file_);  
  if (!s.ok()) { return s; }

  char buf[16];
  Slice result;
  s = Read(9, &result, buf);  
  if (!s.ok() || !result.starts_with(kMagicString)) {
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
ParsedResult* RdbParseImpl::Value() {
  return result_;
}
Status RdbParseImpl::Read(uint64_t len, Slice *result, char *scratch) {
  Status s = sequence_file_->Read(len, result, scratch); 
  if (!s.ok()) {
    return s;
  }
  if (version_ >= 5) {
    const unsigned char *p1 = (const unsigned char *)scratch; 
    check_sum_ = crc64(check_sum_, p1, len); 
  }
  return s;
}
Status RdbParseImpl::LoadExpiretime(uint8_t type, int *expire_time) {
  char buf[8];
  Status s;
  Slice result;
  if (type == kExpireMs) {
    uint64_t t64;  
    s = Read(8, &result, buf); 
    memcpy(&t64, buf, 8);
    *expire_time = static_cast<int>(t64/1000); 
  } else {
    uint32_t t32;
    s = Read(4, &result, buf); 
    memcpy(&t32, buf, 4);
    *expire_time = static_cast<int>(t32);
  }
  return s; 
}
Status RdbParseImpl::LoadEntryType(uint8_t *type) {
  char buf[1];
  Slice result;
  Status s = Read(1, &result, buf); 
  if (!s.ok()) { return s; }
  *type = static_cast<uint8_t>(buf[0]);    
  return s;
}
Status RdbParseImpl::LoadEntryKey(std::string *result) {
  return LoadString(result); 
}

Status RdbParseImpl::LoadEntryDBNum(uint8_t *db_num) {
  char buf[1];
  Slice result;
  Status s = Read(1, &result, buf); 
  if (!s.ok()) { return s; } 
  *db_num = static_cast<uint8_t>(buf[0]);
  return s;
}
Status RdbParseImpl::LoadIntVal(uint32_t type, std::string *result) {
  char buf[8];
  Slice slice_buf; 
  Status s;
  int32_t val;
  if (type == kEncInt8) {
    s = Read(1, &slice_buf, buf);
    if (!s.ok()) { return s; }
    val = static_cast<int8_t>(buf[0]);  
  } else if (type == kEncInt16) {
    s = Read(2, &slice_buf, buf);
    if (!s.ok()) { return s; }
    val = static_cast<int8_t>(buf[0]) | (static_cast<int8_t>(buf[1]) << 8);
  } else if (type == kEncInt32) {
    s = Read(4, &slice_buf, buf);
    if (!s.ok()) { return s; }
    val = static_cast<int8_t>(buf[0]) | (static_cast<int8_t>(buf[1]) << 8) 
      | (static_cast<int8_t>(buf[2]) << 16) | (static_cast<int8_t>(buf[3]) << 24);
  } else {
    return Status::Corruption("no supported type");
  }
  result->assign(std::to_string(val));
  return s; 
}

Status RdbParseImpl::LoadEncLzf(std::string *result) {
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
  bool ret = Read(compress_len, &compress_slice, compress_buf).ok() 
    && (0 != DecompressLzf(compress_buf, compress_len, raw_buf, raw_len));
  if (ret) {
    result->assign(raw_buf, raw_len);
  }
  delete [] compress_buf;
  delete [] raw_buf;
  return ret ? Status::OK() : Status::Corruption("Parse error"); 
}
void RdbParseImpl::ResetResult() {
  result_->expire_time = -1;
  result_->type.clear();
  result_->key.clear(); 
  result_->kv_value.clear(); 
  result_->set_value.clear();
  result_->map_value.clear();
  result_->list_value.clear();

}
Status RdbParseImpl::LoadListZiplist(std::list<std::string> *value) {
  std::string buf;
  Status s = LoadString(&buf); 
  if (!s.ok()) { return s; }
  ZiplistParser ziplist_parser((void *)(buf.c_str()));  
  return ziplist_parser.GetList(value);
}
Status RdbParseImpl::LoadZsetOrHashZiplist(std::map<std::string, std::string> *result) {
  std::string buf;
  Status s = LoadString(&buf);
  if (!s.ok()) { return s; }
  ZiplistParser ziplist_parser((void *)buf.c_str());
  return ziplist_parser.GetZsetOrHash(result);
}
Status RdbParseImpl::LoadZipmap(std::map<std::string, std::string> *result) {
  std::string buf;
  Status s = LoadString(&buf);
  if (!s.ok()) { return s; }
  ZipmapParser zipmap_parser((void *)(buf.c_str()));
  return zipmap_parser.GetMap(result);
}
Status RdbParseImpl::LoadListOrSet(std::list<std::string> *result) {
  uint32_t i = 0, field_size;   
  Status s = LoadFieldLen(&field_size, NULL);  
  if (!s.ok()) { return s; }
  std::string val;
  for (; i < field_size; i++) {
    if (!LoadString(&val).ok()) {
      break;
    } 
    result->push_back(val);
  }
  return i == field_size ? Status::OK() : Status::Corruption("Parse error");
} 
Status RdbParseImpl::LoadHashOrZset(std::map<std::string, std::string> *result) {
  uint32_t i = 0, field_size;   
  Status s = LoadFieldLen(&field_size, NULL);  
  if (!s.ok()) { return s; }
  std::string key, value;
  for (i = 0; i < field_size; i++) {
    if (!LoadString(&key).ok() || !LoadString(&value).ok()) {
      break;
    }
    result->insert({key, value});
  }
  return i == field_size ? Status::OK() : Status::Corruption("Parse error");
}
Status RdbParseImpl::LoadListQuicklist(std::list<std::string> *result) {
  uint32_t i, field_size; 
  Status s = LoadFieldLen(&field_size, NULL);
  if (!s.ok()) { return s; }
  for (i = 0; i < field_size; i++) {
       
  }
  return Status::OK();
}
Status RdbParseImpl::LoadString(std::string *result) {
  uint32_t len;
  bool is_encoded = false;
  Status s = LoadFieldLen(&len, &is_encoded);
  if (!s.ok()) { return s; } 
  if (is_encoded) {
    switch (len) {
      case kEncInt8: 
      case kEncInt16:
      case kEncInt32:
        return LoadIntVal(len, result);      
      case kEncLzf:   
        return LoadEncLzf(result);
      default:
        return Status::Corruption("");
    }  
  }
  char *buf = new char[len];
  s = Read(len, nullptr, buf);  
  if (s.ok()) result->assign(buf, len);
  delete [] buf;
  return s;
}

Status RdbParseImpl::LoadFieldLen(uint32_t *length, bool *is_encoded) {
  char buf[8];      
  Slice result;
  Status s = Read(1, &result, buf);  
  if (!s.ok()) { 
    *length = kLenErr;
    return s; 
  }
  uint32_t len;
  uint8_t type = (buf[0] & 0xc0) >> 6; 
  if (type == k6B) {
    len = buf[0] & 0x3f;
  } else if (type == k14B) {
    s = Read(1, &result, buf + 1); 
    if (!s.ok()) { return s; }
    len = ((buf[0] && 0x3f) << 8) | buf[1];   
  } else if (type == k32B) {
    s = Read(4, &result, buf); 
    if (!s.ok()) { return s; }
    memcpy(&len, buf, 4);  
    len = ntohl(len);
  } else {
    if (is_encoded) { 
      *is_encoded = true; 
    }   
    len = buf[0] & 0x3f;
  }
  *length = len;
  return s;
}
Status RdbParseImpl::LoadEntryValue(uint8_t type) {
  Status s;
  switch (type) {
    case kRdbString:  
      s = LoadString(&(result_->kv_value)); 
      break; 
    case kRdbIntset:
      s = LoadIntset(&(result_->set_value));               
      break;
    case kRdbListZiplist: 
      s = LoadListZiplist(&(result_->list_value));
      break;
    case kRdbHashZipMap:
      s = LoadZipmap(&(result_->map_value));
      break;
    case kRdbZsetZiplist:               
    case kRdbHashZiplist:
      s = LoadZsetOrHashZiplist(&(result_->map_value));
      break;
    case kRdbList:
    case kRdbSet:                
      s = LoadListOrSet(&(result_->list_value));
      break;
    case kRdbHash:
    case kRdbZset:
      s = LoadHashOrZset(&(result_->map_value));
      break;
    case kRdbZset2: //TODO(deng.yihao): add more type 
      s = LoadHashOrZset(&(result_->map_value));
      break;
    case kRdbModule:
      break;
    case kRdbModule2: 
      break;
    case kRdbStreamListpacks:
      break;
    case kRdbListQuicklist:
      break;
    default: 
      return Status::OK(); // skip unrecognised value type
  }
  return s; 
}
Status RdbParseImpl::LoadIntset(std::set<std::string> *result) {
  std::string value;
  if (!LoadString(&value).ok()) {
    return Status::Corruption("Parse error");
  }

  size_t i = 0;
  Intset *int_set = reinterpret_cast<Intset *>((void *)(value.data())); 
  for (; i < int_set->length; i++) {
    int64_t v64;
    if (!int_set->Get(i, &v64).ok()) {
      break; 
    }
    result->emplace(std::to_string(v64));
  } 
  return i == int_set->length ? 
    Status::OK() : Status::Corruption("Parse error");
}
bool RdbParseImpl::Valid() {
  return valid_;
}
Status RdbParseImpl::Next() {
  ResetResult(); 
  while (1) {
    uint8_t type;
    Status s = LoadEntryType(&type);
    if (!s.ok()) { return s; }
    if (type == kEof) {
      valid_ = false;
      return Status::OK(); 
    }
    if (type == kSelectDb) {
      s = LoadFieldLen(&(result_->db_num), NULL);
      if (!s.ok()) { return s; } 
      continue;
    } 

    if (type == kExpireMs || type == kExpireSec) {
      s = LoadExpiretime(type, &(result_->expire_time));
      if (!s.ok()) { return s; }
      s = LoadEntryType(&type);
      if (!s.ok()) { return s; }
    }  
    if (type == kIdle) {
      s = LoadFieldLen(&(result_->idle), NULL);
      if (!s.ok()) { return s; }
      s = LoadEntryType(&type);
      if (!s.ok()) { return s; }
    }
    if (type == kModuleAux) {
      //TODO(dengyihao): add module
      continue; 
    }
    if (type == kResizedb) {
      s = LoadFieldLen(&result_->db_size, NULL);
      if (!s.ok()) { return s; }
      s = LoadFieldLen(&result_->expire_size, NULL);
      if (!s.ok()) { return s; }
      continue; 
    }
    if (type == kAux) {
      s = LoadString(&(result_->aux_field.aux_key)); 
      if (!s.ok()) { return s; }
      s = LoadString(&(result_->aux_field.aux_val));
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

std::string RdbParseImpl::GetTypeName(ValueType type) {
  static std::unordered_map<ValueType, std::string, std::hash<int>> type_map{
    { kRdbString, "string"}, { kRdbList, "list"},
      { kRdbSet, "set"}, { kRdbHashZipMap,"hash"},
      { kRdbZsetZiplist, "zset"}, { kRdbHashZiplist, "hash"},
      { kRdbListZiplist,"list"}, { kRdbIntset, "set"},
      { kRdbHash,"hash"}, { kRdbZsetZiplist,"zset"},
      { kRdbListQuicklist,"list"}, { kRdbStreamListpacks,"stream"},
      { kRdbModule,"module"}, { kRdbModule2,"module"},
  };
  auto it = type_map.find(type);  
  return it != type_map.end() ? it->second : ""; 
}

Status RdbParse::Open(const std::string &path, RdbParse **rdb) {
  *rdb = nullptr;
  RdbParseImpl *impl = new RdbParseImpl(path);
  Status s = impl->Init(); 
  if (!s.ok()) {
    delete impl;
    return s; 
  }
  *rdb = impl;
  return Status::OK();
}

RdbParse::~RdbParse() {
}

}
