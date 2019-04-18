#ifndef __RDBPARSE_H__
#define __RDBPARSE_H__

#include <string> 
#include <map>
#include <list>
#include <set> 
#include "slash/include/slash_status.h" 

using namespace slash; 

struct ParsedResult {
  ParsedResult(): expire_time(-1) {}
  std::string type;
  uint8_t db_num;
  int expire_time;
  std::string key;
  std::string kv_value;
  std::set<std::string> set_value;
  std::map<std::string, std::string> map_value;
  std::list<std::string> list_value;
  void Debug();
};

class RdbParse {
  public:
    static Status Open(const std::string &path, RdbParse **rdb);
    virtual Status Next() = 0;
    virtual bool Valid() = 0; 
    virtual ParsedResult *Value() = 0; 
    RdbParse() = default;
    virtual ~RdbParse();
    RdbParse(const RdbParse&) = delete; 
    RdbParse& operator=(const RdbParse&) = delete;
};
#endif

