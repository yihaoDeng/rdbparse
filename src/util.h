#ifndef __UTIL_H__
#define __UTIL_H__
#include <sys/types.h>
#include <fstream>
#include <errno.h>
#include <stdint.h>
#include <cstddef>
#include <limits.h>

#include "include/status.h"
#include "include/slice.h"
namespace parser {

#undef PLATFORM_IS_LITTLE_ENDIAN
#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif
#include <string.h>

static const bool kLittleEndian = PLATFORM_IS_LITTLE_ENDIAN;
#undef PLATFORM_IS_LITTLE_ENDIAN

void MayReverseMemory(void *p, size_t len);
uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);


class SequentialFile {
  public:
    SequentialFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { setbuf(file_, NULL);  }
    virtual ~SequentialFile() {
      if (file_) {
        fclose(file_);
      }
    }  
    void setUnBuffer() {
      setbuf(file_, NULL);
    }
    Status Read(size_t n, Slice* result, char* scratch) {
      Status s;
      size_t r = fread_unlocked(scratch, 1, n, file_);
      *result = Slice(scratch, r);
      if (r < n) {
        if (feof(file_)) {
          s = Status::EndFile(filename_, "end file");
        } else {
        }
      }
      return s;
    }

    Status Skip(uint64_t n) {
      if (fseek(file_, n, SEEK_CUR)) {
        return Status::IOError(filename_, strerror(errno));
      }
      return Status::OK();
    }

    char *ReadLine(char* buf, int n) {
      return fgets(buf, n, file_);
    }

    Status Close() {
      if (fclose(file_) != 0) {
        return Status::IOError(filename_, strerror(errno));

      }
      file_ = NULL;
      return Status::OK();

    }
  private:
    std::string filename_;
    FILE *file_;
};
Status NewSequentialFile(const std::string& fname, SequentialFile** result);
int string2ll(const char *s, size_t slen, long long *value);
int string2l(const char *s, size_t slen, long *lval); 

}
#endif

