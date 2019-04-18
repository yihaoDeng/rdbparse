#include <iostream>
#include "slash/include/slash_status.h"
#include "include/rdbparse.h"
int main() {
  std::string rdb_path = "rdb/dump2.2.rdb";
  //RdbParseImpl parse(rdb_path); 
  RdbParse *parse;
  Status s = RdbParse::Open(rdb_path, &parse);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    return 1;
  }
  //while (parse->Valid()) {
  //  s = parse->Next(); 
  //  if (!s.ok()) {
  //    break;
  //  }
  //  ParsedResult *value = parse->Value();         
  //  value->Debug();
  //} 
  delete parse;
  return 1;
}
