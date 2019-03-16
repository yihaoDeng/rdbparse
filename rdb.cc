#include <arpa/inet.h>
#include <list>
#include <set>
#include "slash/include/slash_string.h"
#include "rdb.h"
#include "util.h"
#include "intset.h"
#include "lzf.h"
#include "ziplist.h"

using namespace slash;

RdbParser::RdbParser(const std::string &path):
  path_(path), version_(kMagicString.size()), 
  result_(new ParsedResult) {
  }

RdbParser::~RdbParser() {
  delete result_;
};

Status RdbParser::Init() {
  Status s = NewSequentialFile(path_, &sequence_file_);  
  if (!s.ok()) {
    return s;
  }
  char buf[16];
  Slice result;
  s = ReadAndChecksum(9, &result, buf);  

  if (s.ok() && memcmp(result.data(), kMagicString.c_str(), kMagicString.size())) {
    return Status::Incomplete("unsupport rdb head magic");
  } else {
    return s;
  }
  result.remove_prefix(kMagicString.size());  
  long version = 5; 
  if (!string2l(result.data(), result.size(), &version)) {
    return Status::Corruption("unsupport rdb version");
  }
  version_ = static_cast<int>(version_);
  return Status::OK();
}
bool RdbParser::Valid() {
  return false;
}
ParsedResult* Value() {
  return nullptr;
}
Status RdbParser::ReadAndChecksum(uint64_t len, Slice *result, char *scratch) {
  Status s = sequence_file_->Read(len, result, scratch); 
  if (s.ok() && version_ >= 5) {
    const unsigned char *p = reinterpret_cast<const unsigned char *>(scratch); 
    check_sum_ = crc64(check_sum_, p, len); 
  } else {
    return Status::Incomplete("Incomplete data");
  }
}
Status RdbParser::LoadExpiretime(uint8_t type, int *expire_time) {
  Status s;
  char buf[8];
  uint32_t t32;
  uint64_t t64;  
  Slice result;
  if (type == kRdbExpireMs) {
    s = ReadAndChecksum(8, &result, buf); 
    memcpy(&t64, buf, 8);
    *expire_time = static_cast<int>(t64/1000); 
  } else {
    s = ReadAndChecksum(4, &result, buf); 
    memcpy(&t32, buf, 4);
    *expire_time = static_cast<int>(t32);
  }
  return Status::OK();

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
  Slice result; 
  int32_t val;
  if (type == kRdbEncInt8) {
    Status s = ReadAndChecksum(1, &result, buf);
    if (!s.ok()) { return s; }
    val = static_cast<int8_t>(buf[0]);  
  } else if (type == kRdbEncInt16) {
    Status s = ReadAndChecksum(2, &result, buf);
    if (!s.ok()) { return s; }
    val = static_cast<int8_t>(buf[0]) | (static_cast<int8_t>(buf[1]) << 8);
  } else if (type == kRdbEncInt32) {
    Status s = ReadAndChecksum(4, &result, buf);
    if (!s.ok()) { return s; }
    val = static_cast<int8_t>(buf[0]) | (static_cast<int8_t>(buf[1]) << 8) 
      | (static_cast<int8_t>(buf[2]) << 16) | (static_cast<int8_t>(buf[3]) << 24);
  } else {
    return Status::Corruption("no supported type");
  }
  ll2string(buf, sizeof(buf), val);   
  result->assign(buf);
  return Status::OK();
}

Status RdbParser::LoadEncLzf() {
  uint32_t compress_length, len;    
  Status s = LoadFieldLen(&compress_length, NULL); 
  if (!s.ok()) { return s; }
  s = LoadFieldLen(&len, NULL);
  if (!s.ok()) { return s; }

  char *comp_str = new char[compress_length];
  char *str = arena_.AllocateBuffer(len);
  if (!comp_str || !str) { return Status::Corruption("no enough memory to alloc"); }
  Slice result; 
  if (!ReadAndChecksum(compress_length, &result, comp_str).ok()
      || !DecompressLzf(comp_str, compress_length, str, len)) {  
    delete [] comp_str;
    return Status::Corruption("load or decodelzf data failed");
  }
  return Status::OK();
}

Status RdbParser::LoadZiplist(std::list<std::string> *value) {
  std::string result;
  Status s = LoadString(&result); 
  if (!s.ok()) { return s; }
  char *buf = arena_.Value();
  ZiplistParser ziplist_parse(result.c_str());  
  ziplist_parse.Value(value);
}

Status RdbParser::LoadString(std::string *result) {
  bool is_encoded;
  uint32_t len; 
  Status s = LoadFieldLen(&len, &is_encoded);
  if (!s.ok()) { return s; } 
  if (is_encoded) {
    switch (len) {
      case kRdbEncInt8: 
      case kRdbEncInt16:
      case kRdbEncInt32:
        return LoadIntVal(len, result);      
      case kRdbEncLzf:   
        return LoadEncLzf();
      default:
        return Status::Corruption("");
    }  
  }
  Slice result_slice;
  result->resize(len); 
  s = ReadAndChecksum(len, &result_slice, result->c_str());  
  return Status::OK();
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
  return Status::OK();

}
Status RdbParser::LoadEntryValue(uint8_t type) {
  Status s;
  switch (type) {
    case kRdbString:  
      {
        std::string result;
        s = LoadString(&result); 
        if (!s.ok()) { return s; }         
        result_->kv_value.swap(result);
        break; 
      } 
    case kRdbIntset:
      {
        std::set<std::string> result;
        s = LoadIntset(&result);               
        if (!s.ok()) { return s; }              
        result_->set_value.swap(result); 
        break; 
      }
    case kRdbListZiplist: 
      {
        std::list<std::string> result; 
        s = LoadZiplist(&result);
        if (!s.ok()) { return s; }  
        break;
        result_->list_value.swap(result);
      }
                          
    case kRdbZipMap: break;
        
    case kRdbZsetZiplist:               
    case kRdbHashZiplist:
                     break;
    case kRdbList:
    case kRdbSet:                
                     break;
    case kRdbHash:
    case kRdbZset:

                     break;
    default: 
                     return Status::Corruption("Invalid value type");

  }
  return Status::OK();
}
Status RdbParser::LoadIntset(std::set<std::string> *result) {
  char buf[16];
  Status s = LoadString();    
  int64_t v64;
  Intset *int_set = reinterpret_cast<Intset *>(arena_.Value()); 
  for (size_t i = 0; i < int_set->length; i++) {
    s = int_set->Get(i, &v64);
    if (!s.ok()) { return s; } 
    int size = ll2string(buf, sizeof(buf), v64); 
    if (size <= 0) {
      return Status::Corruption("Invalid fieild");
    }
    result->emplace(buf, size);
  } 
  return Status::OK();
}
Status RdbParser::Next() {
  // RdbEntryType type;
  // 1. load expire time
  // 2. load db num;
  // 3. load value
  while (1) {
    uint8_t type;
    Status s = LoadEntryType(&type);
    if (!s.ok()) { return s; }

    if (type == kRdbExpireMs || type == kRdbExpireSec) {
      int expire_time;
      if (!LoadExpiretime(type, &expire_time).ok() 
          || !LoadEntryType(&type).ok()) {
        return Status::Corruption("un complete") ;
      } else {
        result_->expire_time = expire_time;
      }
    }  
    if (type == kRdbSelectDb) {
      uint8_t db_num = 0;  
      s = LoadEntryDBNum(&db_num);
      if (!s.ok()) { return s; } 
      result_->db_num = db_num;
      continue;
    } else if (type == kRdbEof) {
      return s;
    }
    std::string &entry_key; 
    s = LoadEntryKey(entry_key);        
    if (!s.ok()) { return s; } 
    result_->key.assign(arena_.Value(), arena_.ValueSize());

    s = LoadEntryValue(type);

  }
} 

int main() {
  return 1;
}
