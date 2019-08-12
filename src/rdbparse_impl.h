#ifndef __RDBPARSER_IMPL_H__
#define __RDBPARSER_IMPL_H__

#include <set>
#include <list>
#include <map> 
#include <unordered_map>
#include "include/rdbparse.h"
#include "util.h"


namespace parser {

enum ValueType {
  kRdbString = 0, 
  kRdbList = 1,
  kRdbSet = 2, 
  kRdbZset= 3, 
  kRdbHash = 4,
  kRdbZset2 = 5,
  kRdbModule = 6,
  kRdbModule2 = 7, 
  kRdbHashZipmap = 9,    
  kRdbListZiplist = 10,     
  kRdbIntset = 11,
  kRdbZsetZiplist = 12,       
  kRdbHashZiplist = 13,
  kRdbListQuicklist = 14,
  kRdbStreamListpacks = 15
};

class RdbParseImpl : public RdbParse {
  public:
    RdbParseImpl(const std::string& rdb_path); 
    ~RdbParseImpl();

    enum EntryType {
      kExpireSec = 0xfd, 
      kExpireMs = 0xfc,
      kSelectDb = 0xfe,
      kModuleAux = 0xf7,
      kIdle = 0xf8,
      kFreq = 0xf9,
      kAux = 0xfa,
      kResizedb = 0xfb, 
      kEof = 0xff
    };
    enum LengthType {
      k6B = 0,   
      k14B,
      k32B = 0x80,
      k64B = 0x81,
      kEncv = 3,
      kLenErr = std::numeric_limits<uint64_t>::max() 
    };
    enum ModuleType {
      kModuleEof = 0, 
      kModuleSint, 
      kModuleUint, 
      kModuleFloat,
      kModuleDouble,
      kModuleString
    };
    enum EncType {
      kEncInt8 = 0,
      kEncInt16,
      kEncInt32,
      kEncLzf
    }; 
    const static std::string kMagicString;
    const static int kMagicVersion = 5;
    Status Init(); 
    Status Next();
    bool Valid(); 
    ParsedResult *Value(); 
    void ResetResult(); 
    Status Read(uint64_t len, Slice *result, char *scratch);
    Status LoadExpiretime(uint8_t type, int *expire_time); 
    Status LoadEntryType(uint8_t *type);
    Status LoadEntryDBNum(uint8_t *db_num);
    Status LoadEntryKey(std::string *result);     
    Status LoadEntryValue(uint8_t type);

    std::string GetTypeName(ValueType type);
  private: 
    Status LoadLength(uint64_t *length, bool *is_encoded);
    Status LoadIntVal(uint32_t type, std::string *result); 
    Status LoadString(std::string *result);
    Status LoadDouble(double *val);
    Status LoadBinaryDouble(double *val);
    Status LoadIntset(std::set<std::string> *result);
    Status LoadEncLzf(std::string *result);
    Status LoadListZiplist(std::list<std::string> *result);
    Status LoadZsetOrHashZiplist(std::map<std::string, std::string> *result); 
    Status LoadZipmap(std::map<std::string, std::string> *result);
    Status LoadListOrSet(std::list<std::string> *result);
    Status LoadHash(std::map<std::string, std::string> *result);
    Status LoadZset(std::map<std::string, double> *result, bool is_zset2 = false);
    Status LoadListQuicklist(std::list<std::string> *result);
    Status SkipModule();  // skip module   
    Status LoadUint8(uint8_t *ch) {
      char buf[1];
      Status s = Read(1, NULL, buf);
      if (!s.ok()) {
        return s;
      }
      *ch = static_cast<uint8_t>(buf[0]);
      return Status::OK();
    } 
    Status SkipStream();
    Status SkipString(); 
    Status SkipFloat() {
      uint8_t skip_bytes = 0;
      Status s = LoadUint8(&skip_bytes);
      if(s.ok() && skip_bytes < 253) {
        return sequence_file_->Skip(skip_bytes); 
      }
      return s;
    }
    Status SkipBinaryDouble() {
      return sequence_file_->Skip(sizeof(double));
    }
    Status SkipDouble() {
      char buf[16];   
      if (!Read(1, nullptr, buf).ok()) {
        return Status::Corruption("parse load double length error"); 
      }
      uint64_t skip_bytes = 0;
      size_t len = static_cast<size_t>(buf[0]);
      if (len != 255 || len != 254 || len != 253) {
          skip_bytes = len; 
      } 
      return sequence_file_->Skip(skip_bytes);
    }
    std::string path_;
    SequentialFile *sequence_file_;  
    uint64_t check_sum_; 
    int version_;  
    ParsedResult *result_;
    struct Arena;
    bool valid_;
    Arena *arena_;
    RdbParseImpl(const RdbParseImpl&);
    RdbParseImpl& operator=(const RdbParseImpl&);
};
const std::string RdbParseImpl::kMagicString = "REDIS";

}
#endif
