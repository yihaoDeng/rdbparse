#ifndef __RDBPARSE_H__
#define __RDBPARSE_H__

#include <string> 
#include <map>
#include <list>
#include <set> 
#include "status.h" 
#include "slice.h"

namespace parser {

struct AuxKV {
  std::string aux_key;  
  std::string aux_val;
};
struct ParsedResult {
  ParsedResult(): expire_time(-1) {}
  std::string type;
  uint32_t db_num;
  uint32_t idle;
  uint32_t db_size;
  uint32_t expire_size;
  uint32_t freq;
  AuxKV aux_field;
  int expire_time;
  void set_dbnum(uint32_t _db_num) {
    db_num = _db_num;
  } 
  void set_idle(uint32_t _idle) {
    idle = _idle;
  }
  void set_dbsize(uint32_t _db_size) {
    db_size = _db_size;
  }
  void set_expiresize(uint32_t _exire_size) {
    expire_size = _exire_size;
  }
  void set_expiretime(int _expire_time) {
    expire_time = _expire_time;
  }
  void set_freq(uint32_t _freq) {
    freq = _freq;
  }
  void set_auxkv(const std::string &key, const std::string val) {
    aux_field.aux_val = key; 
    aux_field.aux_val = val;
  }
  std::string key;
  std::string kv_value;
  std::set<std::string> set_value;
  std::map<std::string, std::string> map_value;
  std::map<std::string, double> zset_value;
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

}
#endif

