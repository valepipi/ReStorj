//
// Created by ousing9 on 2022/3/9.
//

#include <fcntl.h>
#include <unistd.h>

#include <utility>

#include "file.h"
#include "segment.h"
#include "config.h"

storj::file::file() = default;

storj::file::file(std::string filename, const storj::config &cfg) : name(std::move(filename)), cfg(cfg)
{
    open_read();
}

storj::file::~file()
{
    close();
}

int storj::file::open_read()
{
    fd = ::open(name.c_str(), O_RDONLY);
    return fd;
}

int storj::file::open_write(const std::string &filename)
{
    written_count = 0;
    fd = ::open(filename.c_str(), O_CREAT | O_RDWR);
    return fd;
}

void storj::file::close()
{
    ::close(fd);
    fd = -1;
}

storj::file &storj::file::operator>>(storj::segment &s)
{
    // 以 size 为单位读取文件
    char *segment_data = new char[cfg.segment_size];
    long n;
    if ((n = read(fd, segment_data, cfg.segment_size)) > 0) {
        s.data.assign(segment_data, segment_data + n);
        // 最后一个 segment 补 0
        while (s.data.size() < cfg.segment_size) {
            s.data.emplace_back(0);
        }
    } else {
        close();
    }
    delete[] segment_data;
    return *this;
}

storj::file &storj::file::operator<<(storj::segment &s)
{
    const unsigned long unit = 128 << 10;
    for (unsigned long i = 0; i < s.data.size(); i += unit) {
        unsigned long size = std::min(unit, s.data.size() - i);
        // 写到最后一个 segment 时，可能有冗余数据
        if (written_count / cfg.segment_size == (cfg.file_size + cfg.segment_size - 1) / cfg.segment_size - 1) {
            size = std::min(size, cfg.file_size % cfg.segment_size - i);
        }
        write(fd, s.data.data() + i, size);
        written_count += size;
        if (written_count >= cfg.file_size) {
            break;
        }
    }
    return *this;
}

storj::file::operator bool() const
{
    return fd != -1;
}
