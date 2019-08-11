#include <iostream>
#include "include/rdbparse.h"
void PrintHelp() {
  printf("./parse_test rdbfile.rdb");
}

using namespace parser;
int main(int argc, char* argv[]) {
  if (argc < 2) {
    PrintHelp(); 
    return 1;
  } 
  std::string rdb_path(argv[1]);  
  RdbParse *parse;
  Status s = RdbParse::Open(rdb_path, &parse);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    return 1;
  }
  while (parse->Valid()) {
    s = parse->Next(); 
    if (!s.ok()) {
      std::cout << "Failed:" << s.ToString() << std::endl;
      break;
    }
    ParsedResult *value = parse->Value();         
    value->Debug();
  } 
  delete parse;
  return 1;
}
