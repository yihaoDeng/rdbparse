#ifndef _RDB_PARSER_H_
#define _RDB_PARSER_H_

#include<set>
#include<list>
#include<map> 
#include<unordered_map>
#include "slash/include/env.h"
#include "slash/include/slash_string.h"

using namespace slash;

enum ValueType {
  kRdbString = 0, 
  kRdbList,
  kRdbSet, 
  kRdbZset, 
  kRdbHash,
  kRdbZipMap,    
  kRdbListZiplist,     
  kRdbIntset,
  kRdbZsetZiplist,       
  kRdbHashZiplist
};
struct ParsedResult {
  ValueType type;
  uint8_t db_num;
  int expire_time;
  std::string key; 
  std::string kv_value;
  std::set<std::string> set_value;
  std::map<std::string, std::string> map_value;
  std::list<std::string> list_value;
};

struct Ziplist {
  uint32_t bytes; 
  uint32_t length;
  uint32_t tail;
  char entrys[0];
};


class RdbParser {
  public:
    RdbParser(const std::string& rdb_path); 
    ~RdbParser();

    //enum ZiplistDataFlag {
    //  kZiplistBegin = 254,
    //  kZiplistEnd = 0xff  
    //};
    enum RdbEntryType {
      kRdbExpireSec = 0xfd, 
      kRdbExpireMs = 0xfc,
      kRdbSelectDb = 0xfe,
      kRdbEof = 0xff
    };
    enum LengthType {
      kRdb6B = 0,   
      kRdb14B,
      kRdb32B,
      kRdbEncv,
      kRdbLenErr = UINT32_MAX
    };
    enum EncType {
      kRdbEncInt8 = 0,
      kRdbEncInt16,
      kRdbEncInt32,
      kRdbEncLzf
    }; 
    const static int kZipEncString6b = 0 << 6;
    const static int kZipEncString14b = 1 << 6;
    const static int kZipEncString32b = 2 << 6;
    const static int kZipEncStringMask = 0xc0;  
    const static std::string kMagicString;

    const static int kMagicVersion = 5;
    Status Init(); 
    Status Next();
    bool Valid(); 
    ParsedResult *Value(); 
    Status ReadAndChecksum(uint64_t len, Slice *result, char *scratch);
    Status LoadExpiretime(uint8_t type, int *expire_time); 
    Status LoadEntryType(uint8_t *type);
    Status LoadEntryDBNum(uint8_t *db_num);
    Status LoadEntryKey(std::string *result);     
    Status LoadEntryValue(uint8_t type);

    std::string GetTypeName(ValueType type);
    void ResetResult(); 
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
    RdbParser(const RdbParser&);
    struct Arena;
    Arena *arena_;
    void operator=(const RdbParser&);
};

const std::string RdbParser::kMagicString = "REDIS";
#endif
