#include <iostream>
#include "slash/include/slash_status.h"

using namespace slash;
int main() {
  Status s; 
  if (s.ok()) {
    std::cout << "Hello World" << std::endl;
  }
  return 1;
}
