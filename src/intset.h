#ifndef __INTSET_H__
#define __INTSET_H__
#include "include/status.h"

namespace parser {

struct Intset {
   uint32_t encoding; 
   uint32_t length;
   int8_t content[0];
   Status Get(size_t pos, int64_t *v);
   Status Dump();
};
}
#endif
