#ifndef __RDBPARSER_IMPL_H__
#define __RDBPARSER_IMPL_H__

#include <set>
#include <list>
#include <map> 
#include <unordered_map>
#include "slash/include/env.h"
#include "slash/include/slash_string.h"
#include "include/rdbparse.h"

using namespace slash;

enum ValueType {
  kRdbString = 0, 
  kRdbList = 1,
  kRdbSet = 2, 
  kRdbZset= 3, 
  kRdbHash = 4,
  kRdbHashZipMap = 9,    
  kRdbListZiplist = 10,     
  kRdbIntset = 11,
  kRdbZsetZiplist = 12,       
  kRdbHashZiplist = 13,
  kRdbListQuicklist = 15
};

class RdbParseImpl : public RdbParse {
  public:
    RdbParseImpl(const std::string& rdb_path); 
    ~RdbParseImpl();

    enum RdbEntryType {
      kExpireSec = 0xfd, 
      kExpireMs = 0xfc,
      kSelectDb = 0xfe,
      kEof = 0xff
    };
    enum LengthType {
      k6B = 0,   
      k14B,
      k32B,
      kEncv,
      kLenErr = UINT32_MAX
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
    Status ReadAndChecksum(uint64_t len, Slice *result, char *scratch);
    Status LoadExpiretime(uint8_t type, int *expire_time); 
    Status LoadEntryType(uint8_t *type);
    Status LoadEntryDBNum(uint8_t *db_num);
    Status LoadEntryKey(std::string *result);     
    Status LoadEntryValue(uint8_t type);

    std::string GetTypeName(ValueType type);
  private: 
    Status LoadFieldLen(uint32_t *length, bool *is_encoded);
    Status LoadIntVal(uint32_t type, std::string *result); 
    Status LoadString(std::string *result);
    Status LoadIntset(std::set<std::string> *result);
    Status LoadEncLzf(std::string *result);
    Status LoadListZiplist(std::list<std::string> *result);
    Status LoadZsetOrHashZiplist(std::map<std::string, std::string> *result); 
    Status LoadZipmap(std::map<std::string, std::string> *result);
    Status LoadListOrSet(std::list<std::string> *result);
    Status LoadHashOrZset(std::map<std::string, std::string> *result);

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

#endif
