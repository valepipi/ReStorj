//
// Created by ousing9 on 2022/3/9.
//

#ifndef STORJ_EMULATOR_DATA_PROCESSOR_H
#define STORJ_EMULATOR_DATA_PROCESSOR_H


#include <string>
#include <vector>

#include "erasure_share.h"
#include "file.h"
#include "piece.h"
#include "segment.h"
#include "stripe.h"

namespace storj
{
    class data_processor
    {
        config cfg;

    public:
        data_processor(const config &cfg);

        std::vector<segment> split_file(file &f);
        std::vector<stripe> split_segment(const storj::segment &s) const;
        std::vector<erasure_share> erasure_encode(const storj::stripe &s) const;
        std::vector<piece> merge_to_pieces(std::vector<std::vector<erasure_share>> &s) const;
        std::vector<erasure_share> split_piece(const storj::piece &p) const;
        std::vector<stripe> merge_to_stripes(const std::vector<std::vector<erasure_share>> &s) const;
        segment merge_to_segment(const std::vector<stripe> &stripes) const;
        file merge_to_file(const std::vector<segment> &segments) const;
        std::vector<stripe> repair_stripes_from_erasure_shares(const std::vector<std::vector<erasure_share>> &s) const;
    };
}

#endif //STORJ_EMULATOR_DATA_PROCESSOR_H
