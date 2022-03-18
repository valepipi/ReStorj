//
// Created by ousing9 on 2022/3/9.
//

#include <fcntl.h>
#include <unistd.h>

#include "config.h"
#include "data_processor.h"
#include "file.h"

using namespace storj;

data_processor::data_processor(const config &cfg) : cfg(cfg)
{}

std::vector<segment> data_processor::split_file(file &f)
{
    // TODO: 改写成流式处理
    // 打开文件
    int fd = open(f.name.c_str(), O_RDONLY);
    if (fd == -1) {
        throw "File not exists";
    }

    // 以 size 为单位读取文件
    char segment_data[cfg.segment_size];
    int n;
    while ((n = read(fd, segment_data, cfg.segment_size)) > 0) {
        f.segments.emplace_back(std::vector<char>(segment_data, segment_data + n));
    }

    // 最后一个 segment 补 0
    if (!f.segments.empty()) {
        segment &s = f.segments.back();
        while (s.data.size() < cfg.segment_size) {
            s.data.emplace_back(0);
        }
    }
    return f.segments;
}

std::vector<stripe> data_processor::split_segment(const segment &s) const
{
    // 创建变量，预留空间
    std::vector<stripe> stripes;
    stripes.reserve(s.data.size() / cfg.stripe_size);
    // 以 size 为单位遍历
    for (auto it = s.data.begin(); it < s.data.end(); it += cfg.stripe_size) {
        std::vector<char> stripe_data;
        auto right = it + cfg.stripe_size;
        stripe_data.assign(it, right);
        stripes.emplace_back(stripe_data);
    }
    return stripes;
}

std::vector<erasure_share> data_processor::erasure_encode(const stripe &s) const
{
    // TODO: erasure encode
    // 创建变量，预留空间
    std::vector<erasure_share> shares;
    shares.reserve(s.data.size() / cfg.erasure_share_size);
    // 以 size 为单位遍历
    for (auto it = s.data.begin(); it < s.data.end(); it += cfg.erasure_share_size) {
        std::vector<char> erasure_share_data;
        auto right = it + cfg.erasure_share_size;
        erasure_share_data.assign(it, right);
        shares.emplace_back(erasure_share_data);
    }
    return shares;
}

std::vector<piece> data_processor::merge_to_pieces(std::vector<std::vector<erasure_share>> &s) const
{
    // 粒度为 stripe -> erasure share
    // s[i][j] 是单个 stripe 切分出来的单个 erasure share
    // s[i]    是单个 stripe 切分出来的所有 erasure shares
    // 即，需要交换遍历维度以转换成 piece
    std::vector<piece> pieces;
    pieces.reserve(cfg.n);
    for (int y = 0; y < cfg.n; y++) {
        piece p;
        for (int x = 0; x < s.size(); x++) {
            erasure_share &share = s[x][y];
            share.x_index = x;
            share.y_index = y;
            p.erasure_shares.emplace_back(share);
            p.data.insert(p.data.end(), share.data.begin(), share.data.end());
        }
        pieces.emplace_back(p);
    }
    return pieces;
}

std::vector<erasure_share> data_processor::split_piece(const piece &p) const
{
    // 创建变量，预留空间
    std::vector<erasure_share> shares;
    shares.reserve(p.data.size() / cfg.erasure_share_size);
    // 以 size 为单位遍历
    for (auto it = p.data.begin(); it < p.data.end(); it += cfg.erasure_share_size) {
        std::vector<char> erasure_share_data;
        auto right = it + cfg.erasure_share_size;
        erasure_share_data.assign(it, right);
        shares.emplace_back(erasure_share_data);
    }
    return shares;
}

std::vector<stripe> data_processor::merge_to_stripes(const std::vector<std::vector<erasure_share>> &s) const
{
    // 粒度为 piece -> erasure share
    // s[i][j] 是单个 piece 切分出来的单个 erasure share
    // s[i]    是单个 piece 切分出来的所有 erasure shares
    // 即，需要交换遍历维度以转换成 stripe
    std::vector<stripe> stripes;
    for (int y = 0; y < cfg.n; y++) {
        stripe stripe;
        for (const auto &shares: s) {
            const erasure_share &share = shares[y];
            stripe.data.insert(stripe.data.end(), share.data.begin(), share.data.end());
        }
        stripes.emplace_back(stripe);
    }
    // TODO: erasure decode
    return stripes;
}

segment data_processor::merge_to_segment(const std::vector<stripe> &stripes) const
{
    segment res;
    for (const auto &stripe: stripes) {
        res.data.insert(res.data.end(), stripe.data.begin(), stripe.data.end());
    }
    return res;
}

file data_processor::merge_to_file(const std::vector<segment> &segments) const
{
    file res;
    res.segments = segments;
    return res;
}

std::vector<stripe> data_processor::repair_stripes_from_erasure_shares(const std::vector<std::vector<erasure_share>> &s) const
{
    // TODO: implementation
}
