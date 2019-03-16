#include "rdb.h"

RdbParser::RdbParser(const std::string &rdb_path):
  rdb_path_(path), result_(new ParsedResult) {

  }

RdbParser::~RdbParser() {
  delete result_;
};

Status RdbParser::Init() {

}

Status RdbParser::Next() {

} 

