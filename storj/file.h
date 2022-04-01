//
// Created by ousing9 on 2022/3/9.
//

#ifndef STORJ_EMULATOR_FILE_H
#define STORJ_EMULATOR_FILE_H


#include <vector>
#include <string>

#include "config.h"
#include "segment.h"

namespace storj
{
    class file
    {
    private:
        unsigned long written_count = 0;

    public:
        boost::uuids::uuid id{};
        std::string name;
        config cfg;
        int fd = -1;

        file();
        file(std::string filename, const config &cfg);
        virtual ~file();

        int open_read();
        int open_write(const std::string &filename);
        void close();
        file &operator>>(segment &s);
        file &operator<<(segment &s);
        explicit operator bool() const;
    };
}

#endif //STORJ_EMULATOR_FILE_H
