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
    printf("]\n"); } else if (this->type == "list") {
    printf("value: [\n");  
    auto& list_value = this->list_value; 
    for (auto it = list_value.begin(); it != list_value.end();) {
      printf("%s", (*it).c_str());
      it++;
      if (it != list_value.end()) {
        printf(", ");
      }
    }
    printf("]\n"); 
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
  } else if (this->type == "zset") {
    printf("value:["); 
    auto &values = this->zset_value;
    for(auto it = values.begin(); it != values.end();) {
      printf("%s -> %lf", it->first.c_str(), it->second); 
      it++; 
      if (it != values.end()) {
        printf(", ");
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
    uint8_t *p1 = reinterpret_cast<uint8_t *>(scratch); 
    check_sum_ = crc64(check_sum_, p1, len); 
  }
  return s;
}
Status RdbParseImpl::LoadExpiretime(uint8_t type, int *expire_time) {
  char buf[8];
  Status s;
  if (type == kExpireMs) {
    uint64_t t64;  
    s = Read(8, nullptr, buf); 
    memcpy(&t64, buf, 8);
    *expire_time = static_cast<int>(t64/1000); 
  } else {
    uint32_t t32;
    s = Read(4, nullptr, buf); 
    memcpy(&t32, buf, 4);
    *expire_time = static_cast<int>(t32);
  }
  return s; 
}
Status RdbParseImpl::LoadEntryType(uint8_t *type) {
  char buf[1];
  Status s = Read(1, nullptr, buf); 
  if (!s.ok()) { return s; }
  *type = static_cast<uint8_t>(buf[0]);    
  return s;
}
Status RdbParseImpl::LoadEntryKey(std::string *result) {
  return LoadString(result); 
}

Status RdbParseImpl::LoadIntVal(uint32_t type, std::string *result) {
  char buf[8];
  Status s;
  int32_t val;
  if (type == kEncInt8) {
    if (!Read(1, nullptr, buf).ok()) {
      return Status::Corruption("parse int val err");
    }
    val = static_cast<int8_t>(buf[0]);  
  } else if (type == kEncInt16) {
    if (!Read(2, nullptr, buf).ok()) { 
      return Status::Corruption("parse int val err");
    }
    uint16_t t = static_cast<uint8_t>(buf[0]) | (static_cast<uint8_t>(buf[1]) << 8);
    val = static_cast<int16_t>(t);
  } else if (type == kEncInt32) {
    if (Read(4, nullptr, buf).ok()) {
      return Status::Corruption("parse int val err");
    }
    val = static_cast<uint8_t>(buf[0]) | (static_cast<uint8_t>(buf[1]) << 8) 
      | (static_cast<uint8_t>(buf[2]) << 16) | (static_cast<uint8_t>(buf[3]) << 24);
  } else {
    return Status::Corruption("no supported type");
  }
  result->assign(std::to_string(val));
  return s; 
}

Status RdbParseImpl::LoadEncLzf(std::string *result) {
  uint64_t raw_len, compress_len;    
  if (!LoadLength(&compress_len, NULL).ok()) {
    return Status::Corruption("parse enclzf compress_len error");          
  }
  if (!LoadLength(&raw_len, NULL).ok()) {
    return Status::Corruption("parse enclzf raw_len error");          
  }

  char *compress_buf = new char[compress_len];
  char *raw_buf = new char[raw_len]; 
  if (!compress_buf || !raw_buf) { 
    return Status::Corruption("no enough memory to alloc"); 
  }
  bool ret = Read(compress_len, nullptr, compress_buf).ok() 
    && (0 != DecompressLzf(compress_buf, compress_len, raw_buf, raw_len));
  if (ret) {
    result->assign(raw_buf, raw_len);
  }
  delete [] compress_buf;
  delete [] raw_buf;
  return ret ? Status::OK() : Status::Corruption("parse enclzf error"); 
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
  if (!LoadString.ok()) {
    return Status::Corruption("parse list ziplist err");
  }
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
  uint32_t i; 
  uint64_t field_size;   
  Status s = LoadLength(&field_size, NULL);  
  if (!s.ok()) { return s; }
  std::string val;
  for (i = 0; i < field_size; i++) {
    if (!LoadString(&val).ok()) {
      break;
    } 
    result->push_back(val);
  }
  return i == field_size ? Status::OK() : Status::Corruption("Parse error");
} 
Status RdbParseImpl::LoadHash(std::map<std::string, std::string> *result) {
  uint32_t i;
  uint64_t field_size;   
  Status s = LoadLength(&field_size, NULL);  
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
Status RdbParseImpl::LoadZset(std::map<std::string, double> *result, bool zset2) {
  uint32_t i; 
  uint64_t field_size;   
  Status s = LoadLength(&field_size, NULL);  
  if (!s.ok()) { return s; }
  std::string key;
  double val;
  for (i = 0; i < field_size; i++) {
    if (!LoadString(&key).ok()) {
      break;
    }
    s = zset2 ? LoadBinaryDouble(&val) : LoadDouble(&val);
    if (!s.ok()) { break; }

    result->insert({key, val});
  }
  return i == field_size ? Status::OK() : Status::Corruption("Parse error");
}  
Status RdbParseImpl::LoadListQuicklist(std::list<std::string> *result) {
  uint32_t i; 
  uint64_t field_size; 
  Status s = LoadLength(&field_size, NULL);
  if (!s.ok()) { return s; }
  for (i = 0; i < field_size; i++) {
    s = LoadListZiplist(result); 
    if (!s.ok()) { break; }
  }
  return i == field_size ? Status::OK() : Status::Corruption("parse Corruption");
}

Status RdbParseImpl::SkipModule() {
  uint64_t id;
  if (!LoadLength(&id, NULL).ok()) {
    return Status::Corruption("parse module id error");
  }
  if (!LoadLength(&id, NULL).ok()) {
    return Status::Corruption("parse module opera id error");
  }
  while (id != kModuleEof) {
    if (id == kModuleSint || id == kModuleUint) {
      uint64_t len;
      if (!LoadLength(&len, NULL).ok()) {
        return Status::Corruption("parse module data int/uint error");
      } 
    } else if (id == kModuleFloat) {
      if (!SkipFloat().ok()) {
        return Status::Corruption("parse module data float error");
      }  
    } else if (id == kModuleDouble) {
      if (!SkipBinaryDouble().ok()) {
        return Status::Corruption("parse module data double error");
      } 
    } else if (id == kModuleString) {
      if (!SkipString().ok()) {
        return Status::Corruption("parse module data string error");
      }
    }
    if (!LoadLength(&id, NULL).ok()) {
      return Status::Corruption("parse module opera id error");
    }
  }
  return Status::OK();
}
Status RdbParseImpl::SkipStream() {
  uint64_t len; 
  const std::string err_msg = "skip stream error";
  if (!LoadLength(&len, NULL).ok()) {
    return Status::Corruption(err_msg);
  }
  for (uint64_t i = 0; i < len; i++) {
    if (!SkipString().ok() || !SkipString().ok()) {
      return Status::Corruption(err_msg);
    }
  }
  for (uint64_t i = 0; i < 4; i++) {
    if (!LoadLength(&len, NULL).ok()) {
      return Status::Corruption(err_msg);
    }
  }
  uint64_t cgroups = len;
  for (uint64_t i = 0; i < cgroups; i++) {
     uint64_t pends, consumers;
     if (!SkipString().ok()) {
        return Status::Corruption(err_msg); 
     } 

     // skip, donnot care value
     for (uint64_t i = 0; i < 3; i++) {
       if (!(LoadLength(&pends, NULL).ok())) {
         return Status::Corruption(err_msg);
       }
     }
     for (uint64_t j = 0; j < pends; j++) {
       uint64_t length;
       if (!sequence_file_->Skip(16 + 8).ok()
           || !LoadLength(&length, NULL).ok()) {
          return Status::Corruption(err_msg);
       }
     }
     if (!LoadLength(&consumers, NULL).ok()) {
        return Status::Corruption(err_msg); 
     }
     for (uint64_t j = 0; j < consumers; j++) {
       uint64_t skip_blocks; 
       if (!SkipString().ok() 
           || !sequence_file_->Skip(8).ok()
           || !LoadLength(&skip_blocks, NULL).ok()
           || !sequence_file_->Skip(skip_blocks * 16).ok()) {
          return Status::Corruption(err_msg);
       }
     }
  }
  return Status::OK();

} 
Status RdbParseImpl::LoadString(std::string *result) {
  uint64_t len;
  bool is_encoded = false;
  Status s = LoadLength(&len, &is_encoded);
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

Status RdbParseImpl::LoadDouble(double *val) {
  char buf[16];   
  if (!Read(1, nullptr, buf).ok()) {
    return Status::Corruption("parse load double length error"); 
  }

  size_t len = static_cast<size_t>(buf[0]);
  switch (len) {
    case 255: 
      *val = std::numeric_limits<double>::max(); 
      break;
    case 254:
      *val = std::numeric_limits<double>::min();
      break;
    case 253:
      *val = std::numeric_limits<double>::quiet_NaN();
      break;
    default: 
      {
        if (Read(static_cast<uint64_t>(len), nullptr, buf).ok()
            && string2d(buf, len, val)) {
          return Status::OK();
        } else {
          return Status::Corruption("string2double failed");
        }
      }        
  }
  return Status::OK();
}
Status RdbParseImpl::LoadBinaryDouble(double *val) {
  char *ptr = reinterpret_cast<char *>(val);
  Status s = Read(sizeof(*val), nullptr, ptr); 
  if (!s.ok()) { return s; }
  MayReverseMemory(ptr, sizeof(*val));
  return Status::OK();
}
Status RdbParseImpl::SkipString() {
  uint64_t len, skip_bytes;
  bool is_encoded = false;
  if (!LoadLength(&len, &is_encoded).ok()) {
    return Status::Corruption("skip string error");
  }
  skip_bytes = len;
  if (is_encoded) {
    switch (len) {
      case kEncInt8: 
        skip_bytes = 1;
        break;
      case kEncInt16:
        skip_bytes = 2;
        break;
      case kEncInt32:
        skip_bytes = 4; 
        break;
      case kEncLzf:   
        {
          uint64_t cl, l;
          if (!LoadLength(&cl, NULL).ok() 
              || !LoadLength(&l, NULL).ok()) {
            return Status::Corruption("skip string error");
          }
          skip_bytes = cl; 
        }
        break;
      default:
        return Status::Corruption("");
    }  
  }
  return sequence_file_->Skip(skip_bytes);
}


Status RdbParseImpl::LoadLength(uint64_t *length, bool *is_encoded) {
  char buf[8];      
  Status s = Read(1, nullptr, buf);  
  if (!s.ok()) { 
    *length = kLenErr;
    return s; 
  }
  uint8_t type = (static_cast<uint8_t>(buf[0]) & 0xc0) >> 6;
  if (type == k6B) {
    *length = static_cast<uint8_t>(buf[0]) & 0x3f;
  } else if (type == k14B) {
    s = Read(1, nullptr, buf + 1); 
    if (!s.ok()) { 
      return s; 
    }
    *length = ((static_cast<uint8_t>(buf[0]) & 0x3f) << 8) | (static_cast<uint8_t>(buf[1]) & 0xff);   
  } else if (type == kEncv){
    if (is_encoded) { 
      *is_encoded = true; 
    }   
    *length = buf[0] & 0x3f;
  } 
  uint8_t flag = static_cast<uint8_t>(buf[0]);
  if (flag == k32B) {
    uint32_t l;
    s = Read(4, nullptr, buf); 
    if (!s.ok()) { 
      return s; 
    }
    memcpy(&l, buf, 4);  
    MayReverseMemory(static_cast<void *>(&l), 4);
    *length = static_cast<uint64_t>(l);
  } else if (flag == k64B) {
    s = Read(8, nullptr, buf); 
    if (!s.ok()) { 
      return s; 
    }
    memcpy(length, buf, 8);  
    MayReverseMemory(static_cast<void *>(length), 8);
  }
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
    case kRdbHashZipmap:
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
      s = LoadHash(&(result_->map_value));
      break;
    case kRdbZset:
      s = LoadZset(&(result_->zset_value));
      break;
    case kRdbZset2: //TODO(deng.yihao): add more type 
      s = LoadZset(&(result_->zset_value), true); 
      break;
    case kRdbModule:
      s = Status::Corruption("parse key module error");
    case kRdbModule2: 
      s = SkipModule();
      break;
    case kRdbStreamListpacks:
      s = SkipStream();
      break;
    case kRdbListQuicklist:
      s = LoadListQuicklist(&(result_->list_value));
      break;
    default: 
      s = Status::OK(); // skip unrecognised value type
  }
  return s; 
}
Status RdbParseImpl::LoadIntset(std::set<std::string> *result) {
  std::string value;
  if (!LoadString(&value).ok()) {
    return Status::Corruption("Parse intset error");
  }

  size_t i;
  Intset *int_set = reinterpret_cast<Intset *>((void *)(value.data())); 
  for (i = 0; i < int_set->length; i++) {
    int64_t v64;
    if (!int_set->Get(i, &v64).ok()) {
      break; 
    }
    result->emplace(std::to_string(v64));
  } 
  return i == int_set->length ? 
    Status::OK() : Status::Corruption("Parse intset error");
}
bool RdbParseImpl::Valid() {
  return valid_;
}
Status RdbParseImpl::Next() {
  ResetResult(); 
  Status s;
  while (1) {
    uint8_t type;
    if (!LoadEntryType(&type).ok()) {
      return Status::Corruption("parse type error");
    }
    // set expire time
    if (type == kExpireMs || type == kExpireSec) {
      int expire_time;
      if (!LoadExpiretime(type, &expire_time).ok()) {
        return Status::Corruption("parse expire time error");
      }
      result_->set_expiretime(expire_time);
      if (!LoadEntryType(&type).ok()) {
        return Status::Corruption("parse type errror");
      }
    }  

    if (type == kIdle) {
      uint64_t idle;
      if (!LoadLength(&idle, NULL).ok()) {
        return Status::Corruption("parse idle error");
      };
      result_->set_idle(static_cast<uint32_t>(idle));
      if (!LoadEntryType(&type).ok()) {
        return Status::Corruption("parse type error");
      }
    }
    if (type == kFreq) {
      uint64_t freq; 
      if (!LoadLength(&freq, NULL).ok()) {
        return Status::Corruption("parse idle error");
      };
      result_->set_freq(static_cast<uint32_t>(freq));
      if (!LoadEntryType(&type).ok()) {
        return Status::Corruption("parse type error");
      }
    }
    if (type == kSelectDb) {
      uint64_t select_db;
      if (!LoadLength(&(select_db), NULL).ok()) {
        return Status::Corruption("parse selectdb db_num error");
      }
      result_->set_dbnum(static_cast<uint32_t>(select_db));
      continue;
    } 
    if (type == kAux) {
      std::string k, v;
      if (!LoadString(&k).ok() || !LoadString(&v).ok()) {
        return Status::Corruption("parse aux kv error");
      } 
      result_->set_auxkv(k, v); 
      continue;
    }
    if (type == kResizedb) {
      uint64_t db_size, expire_size;
      if (!LoadLength(&db_size, NULL).ok() || !LoadLength(&expire_size, NULL).ok()) {
        return Status::Corruption("parse resize error");
      } 
      result_->set_dbsize(static_cast<uint32_t>(db_size));
      result_->set_expiresize(static_cast<uint32_t>(expire_size));
      continue; 
    }
    if (type == kModuleAux) {
      if (!SkipModule().ok()) {
        return Status::Corruption("parse module error");
      }
      continue; 
    }
    if (type == kEof) {
      valid_ = false;
      return Status::OK(); 
    }
    // load object
    s = LoadEntryKey(&(result_->key));        
    if (!s.ok()) { return s; } 
    result_->type = GetTypeName(ValueType(type));
    s = LoadEntryValue(type);
    return s;
  }
} 

std::string RdbParseImpl::GetTypeName(ValueType type) {
  static std::unordered_map<ValueType, std::string, std::hash<int>> type_map {
    { kRdbString, "string"}, { kRdbList, "list"},
      { kRdbSet, "set"}, { kRdbHashZipmap,"hash"},
      { kRdbZset, "zset"}, { kRdbHashZiplist, "hash"},
      { kRdbListZiplist,"list"}, { kRdbIntset, "set"},
      { kRdbHash,"hash"}, { kRdbZsetZiplist,"zset"},
      { kRdbListQuicklist,"list"}, { kRdbStreamListpacks,"stream"},
      { kRdbModule,"module"}, { kRdbModule2,"module"},
      { kRdbZset2, "zset"},
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


} // namespace parser
