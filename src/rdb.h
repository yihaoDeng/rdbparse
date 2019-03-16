#ifndef _RDB_PARSER_H_
#define _RDB_PARSER_H_
#include<set>
#include<map> 

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
  uint32_t db_num;
  int64_t expire_time;
  std::string key; 
  std::string kv_value;
  std::set<std::string> set_value;
  std::map<std::string> map_value;
};
class RdbParser {
  public:
    RdbParser(const std::string& rdb_path); 
    ~RdbParser()
    Status Init(); 

    enum LengthType {
      kRdb6B = 0,   
      kRdb14B,
      kRdb32B,
      kRdbEncv,
      kRdbLenErr = UINT_MAX
    };
    enum EncType {
      kRdbEncInt8 = 0,
      kRdbEncInt16,
      kRdbEncInt32,
      kRdbEncLzf
    }; 
    Status Next();
    bool Valid(); 
    ParsedResult *Value(); 
  private: 
    std::string rdb_path_;
    ParsedResult *result_;
    RdbParser(const RdbParser&);
    void RdbParser(const RdbParser&);
};
#endif
