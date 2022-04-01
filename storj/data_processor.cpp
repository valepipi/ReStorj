//
// Created by ousing9 on 2022/3/9.
//

#include "config.h"
#include "data_processor.h"
#include "file.h"

using namespace storj;

data_processor::data_processor(const config &cfg) : cfg(cfg)
{}

std::vector<stripe> data_processor::split_segment(segment &s) const
{
    // 创建变量，预留空间
    std::vector<stripe> stripes;
    stripes.reserve(s.data.size() / cfg.stripe_size);
    // 以 size 为单位遍历
    for (auto it = s.data.begin(); it < s.data.end(); it += cfg.stripe_size) {
        std::vector<char> stripe_data;
        auto right = it + cfg.stripe_size;
        stripe_data.reserve(right - it);
        stripe_data.insert(stripe_data.end(), std::make_move_iterator(it), std::make_move_iterator(right));
        stripes.emplace_back(stripe_data);
    }
    s.data.clear();
    return stripes;
}

std::vector<erasure_share> data_processor::erasure_encode(stripe &s) const
{
    // TODO: erasure encode
    // 创建变量，预留空间
    std::vector<erasure_share> shares;
    shares.reserve(cfg.n);
    // 以 size 为单位遍历
    for (auto it = s.data.begin(); it < s.data.end(); it += cfg.erasure_share_size) {
        std::vector<char> erasure_share_data;
        auto right = it + cfg.erasure_share_size;
        erasure_share_data.reserve(right - it);
        erasure_share_data.insert(erasure_share_data.end(), std::make_move_iterator(it), std::make_move_iterator(right));
        shares.emplace_back(erasure_share_data);
    }
    // 未实现 erasure encode，临时填充
    while (shares.size() < cfg.n) {
        shares.emplace_back(std::vector<char>(cfg.erasure_share_size, '\0'));
    }
    s.data.clear();
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
        p.erasure_shares.reserve(s.size());
        p.data.reserve(s.size() * cfg.erasure_share_size);
        for (auto &shares: s) {
            erasure_share &share = shares[y];
            p.data.insert(p.data.end(), std::make_move_iterator(share.data.begin()), std::make_move_iterator(share.data.end()));
            p.erasure_shares.emplace_back(share);
        }
        pieces.emplace_back(p);
    }
    return pieces;
}

std::vector<erasure_share> data_processor::split_piece(piece &p) const
{
    // 创建变量，预留空间
    std::vector<erasure_share> shares;
    shares.reserve(p.data.size() / cfg.erasure_share_size);
    // 以 size 为单位遍历
    for (auto it = p.data.begin(); it < p.data.end(); it += cfg.erasure_share_size) {
        std::vector<char> erasure_share_data;
        auto right = it + cfg.erasure_share_size;
        erasure_share_data.reserve(right - it);
        erasure_share_data.insert(erasure_share_data.end(), std::make_move_iterator(it), std::make_move_iterator(right));
        shares.emplace_back(erasure_share_data);
    }
    p.data.clear();
    return shares;
}

std::vector<stripe> data_processor::merge_to_stripes(std::vector<std::vector<erasure_share>> &s) const
{
    // 粒度为 piece -> erasure share
    // s[i][j] 是单个 piece 切分出来的单个 erasure share
    // s[i]    是单个 piece 切分出来的所有 erasure shares
    // 即，需要交换遍历维度以转换成 stripe
    std::vector<stripe> stripes;
    // 未实现 erasure decode，临时处理数据，只需前 k 个 piece
    for (int x = 0; x < cfg.segment_size / cfg.stripe_size; x++) {
        stripe stripe;
        stripe.data.reserve(cfg.stripe_size);
        for (int y = 0; y < cfg.k; y++) {
            erasure_share &share = s[y][x];
            stripe.data.insert(stripe.data.end(), std::make_move_iterator(share.data.begin()), std::make_move_iterator(share.data.end()));
        }
        stripes.emplace_back(stripe);
    }
    // TODO: erasure decode
    return stripes;
}

segment data_processor::merge_to_segment(std::vector<stripe> &stripes) const
{
    segment res;
    for (auto &stripe: stripes) {
        res.data.insert(res.data.end(), std::make_move_iterator(stripe.data.begin()), std::make_move_iterator(stripe.data.end()));
    }
    return res;
}

std::vector<stripe> data_processor::repair_stripes_from_erasure_shares(const std::vector<std::vector<erasure_share>> &s) const
{
    // TODO: implementation
}
