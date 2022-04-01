//
// Created by ousing9 on 2022/3/9.
//

#ifndef STORJ_EMULATOR_ERASURE_SHARE_H
#define STORJ_EMULATOR_ERASURE_SHARE_H


#include <vector>

#include <boost/uuid/uuid.hpp>

namespace storj
{
    struct erasure_share
    {
        std::vector<char> data;

        erasure_share();
        erasure_share(std::vector<char> data);
    };
}

#endif //STORJ_EMULATOR_ERASURE_SHARE_H
