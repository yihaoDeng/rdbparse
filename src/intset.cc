#include <stdio.h>
#include "intset.h"
#include "util.h"

Status Intset::Get(size_t pos, int64_t *v) {
  if (pos >= length) {
    return Status::Incomplete("uncomplet intsetk"); 
  }
  if (encoding == sizeof(int64_t)) {
    int64_t v64;
    memcpy(&v64, (int64_t*)content + pos, sizeof(int64_t));
    memrev64ifbe(&v64);
    *v = v64;
  } else if (encoding == sizeof(int32_t)) {
    int32_t v32;
    memcpy(&v32, (int32_t*)content + pos, sizeof(int32_t));
    memrev32ifbe(&v32);
    *v = v32;
  } else {
    int16_t v16;  
    memcpy(&v16, (int16_t*)content + pos, sizeof(int16_t));
    memrev16ifbe(&v16);
    *v = v16;
  }
  return Status::OK();
}

Status Intset::Dump() {
  printf("encoding: %d\n", encoding);
  printf("length: %d\n", length);
  int64_t v;
  printf("element { ");
  for (size_t i = 0; i < length; i++) {
    Status s = Get(i, &v); 
    if (!s.ok()) {
      return s;
    }
    printf("%ld\t", v);
  }
  printf("}\n");
  return Status::OK();
}
