//
// Created by ousing9 on 2022/3/9.
//

#include <algorithm>
#include <fcntl.h>
#include <sqlite3.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "config.h"
#include "data_manager.h"
#include "data_processor.h"
#include "file.h"

using namespace storj;

data_manager::data_manager()
{
    init();
}

data_manager::~data_manager()
{
    // 关闭数据库
    sqlite3_close_v2(sql);
}

void data_manager::init()
{
    // 初始化数据库
    init_db();
    // 初始化存储节点
    init_storage_nodes();
}

void data_manager::init_db()
{
    const char *sql_create_table_file = "create table if not exists \"file\"\n"
                                        "(\n"
                                        "    \"id\"                 varchar(64) primary key not null,\n"
                                        "    \"file_name\"          varchar(255)            not null,\n"
                                        "    \"file_size\"          int(11)                 not null,\n"
                                        "    \"segment_size\"       int(11)                 not null,\n"
                                        "    \"stripe_size\"        int(11)                 not null,\n"
                                        "    \"erasure_share_size\" int(11)                 not null,\n"
                                        "    \"k\"                  int(11)                 not null,\n"
                                        "    \"m\"                  int(11)                 not null,\n"
                                        "    \"n\"                  int(11)                 not null\n"
                                        ");";
    const char *sql_create_table_segment = "create table if not exists \"segment\"\n"
                                           "(\n"
                                           "    \"id\"      varchar(64) primary key not null,\n"
                                           "    \"index\"   int(11)                 not null,\n"
                                           "    \"file_id\" varchar(64)             not null\n"
                                           ");";
    const char *sql_create_table_stripe = "create table if not exists \"stripe\"\n"
                                          "(\n"
                                          "    \"id\"         varchar(64) primary key not null,\n"
                                          "    \"index\"      int(11)                 not null,\n"
                                          "    \"segment_id\" varchar(64)             not null\n"
                                          ");";
    const char *sql_create_table_erasure_share = "create table if not exists \"erasure_share\"\n"
                                                 "(\n"
                                                 "    \"id\"        varchar(64) primary key not null,\n"
                                                 "    \"x_index\"   int(11)                 not null,\n"
                                                 "    \"y_index\"   int(11)                 not null,\n"
                                                 "    \"stripe_id\" varchar(64)             not null,\n"
                                                 "    \"piece_id\"  varchar(64)             not null\n"
                                                 ");";
    const char *sql_create_table_piece = "create table if not exists \"piece\"\n"
                                         "(\n"
                                         "    \"id\"              varchar(64) primary key not null,\n"
                                         "    \"storage_node_id\" int(11)                 not null\n"
                                         ");";
    const char *sql_create_table_storage_node = "create table if not exists \"storage_node\"\n"
                                                "(\n"
                                                "    \"id\" int(11) primary key not null\n"
                                                ");";
    // 打开数据库
    sqlite3_open_v2("storj.db", &sql, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_SHAREDCACHE, nullptr);
    // 创建数据表
    sqlite3_exec(sql, sql_create_table_file, nullptr, nullptr, nullptr);
    sqlite3_exec(sql, sql_create_table_segment, nullptr, nullptr, nullptr);
    sqlite3_exec(sql, sql_create_table_stripe, nullptr, nullptr, nullptr);
    sqlite3_exec(sql, sql_create_table_erasure_share, nullptr, nullptr, nullptr);
    sqlite3_exec(sql, sql_create_table_piece, nullptr, nullptr, nullptr);
    sqlite3_exec(sql, sql_create_table_storage_node, nullptr, nullptr, nullptr);
}

void data_manager::init_storage_nodes()
{
    // 添加数据库记录
    int node_count;
    {
        const char *sql_select = "select count(1)\n"
                                 "from \"storage_node\";";
        sqlite3_stmt *stmt;
        sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr);
        sqlite3_step(stmt);
        node_count = sqlite3_column_int(stmt, 1);
        sqlite3_finalize(stmt);
    }
    // 补足节点数量
    for (int i = node_count; i < storage_node_num; i++) {
        boost::uuids::random_generator uuid_v4;
        const std::string &node_id = to_string(uuid_v4());
        const char *sql_insert = "insert into \"storage_node\"(\"id\")\n"
                                 "values (?);";
        sqlite3_stmt *stmt;
        sqlite3_prepare_v2(sql, sql_insert, -1, &stmt, nullptr);
        sqlite3_bind_text(stmt, 1, node_id.c_str(), node_id.length(), nullptr);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    // 查出所有节点，加入到 set 中
    {
        boost::uuids::string_generator sg;
        const char *sql_select = "select \"id\"\n"
                                 "from \"storage_node\";";
        sqlite3_stmt *stmt;
        sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr);
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const boost::uuids::uuid &node_id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 1)));
            storage_nodes.emplace(node_id);
        }
        sqlite3_finalize(stmt);
    }
    // 创建目录
    mkdir(storage_node_base_path.c_str(), 0755);
    for (const auto &node: storage_nodes) {
        mkdir(get_storage_node_path(node).c_str(), 0755);
    }
}

std::string data_manager::get_storage_node_path(const storage_node &node)
{
    return get_storage_node_path(to_string(node.id));
}

std::string data_manager::get_storage_node_path(const std::string &node_id)
{
    return storage_node_base_path + node_id;
}

std::string data_manager::get_piece_path(const std::string &node_id, const std::string &piece_id)
{
    return get_storage_node_path(node_id) + "/" + piece_id;
}

std::string data_manager::get_piece_path(const std::string &piece_id)
{
    const piece &piece = db_select_piece(piece_id);
    return get_piece_path(to_string(piece.storage_node_id), piece_id);
}

void data_manager::db_insert_file(const file &f)
{
    const std::string &file_id = to_string(f.id);
    const char *sql_insert = "insert into \"file\"(\"id\", \"file_name\", \"file_size\", \"segment_size\", \"stripe_size\", \"erasure_share_size\", \"k\", \"m\", \"n\")\n"
                             "values (?, ?, ?, ?, ?, ?, ?, ?, ?);";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(sql, sql_insert, -1, &stmt, nullptr);
    sqlite3_bind_text(stmt, 1, file_id.c_str(), file_id.length(), nullptr);
    sqlite3_bind_text(stmt, 2, f.name.c_str(), f.name.length(), nullptr);
    sqlite3_bind_int(stmt, 3, f.cfg.file_size);
    sqlite3_bind_int(stmt, 4, f.cfg.segment_size);
    sqlite3_bind_int(stmt, 5, f.cfg.stripe_size);
    sqlite3_bind_int(stmt, 6, f.cfg.erasure_share_size);
    sqlite3_bind_int(stmt, 7, f.cfg.k);
    sqlite3_bind_int(stmt, 8, f.cfg.m);
    sqlite3_bind_int(stmt, 9, f.cfg.n);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

void data_manager::db_insert_segment(const segment &s)
{
    const std::string &segment_id = to_string(s.id);
    const std::string &file_id = to_string(s.file_id);
    const char *sql_insert = "insert into \"segment\"(\"id\", \"y_index\", \"file_id\")\n"
                             "values (?, ?, ?);";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(sql, sql_insert, -1, &stmt, nullptr);
    sqlite3_bind_text(stmt, 1, segment_id.c_str(), segment_id.length(), nullptr);
    sqlite3_bind_text(stmt, 2, file_id.c_str(), file_id.length(), nullptr);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

void data_manager::db_insert_stripe(const stripe &s)
{
    const std::string &stripe_id = to_string(s.id);
    const std::string &segment_id = to_string(s.segment_id);
    const char *sql_insert = "insert into \"stripe\"(\"id\", \"y_index\", \"segment_id\")\n"
                             "values (?, ?, ?);";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(sql, sql_insert, -1, &stmt, nullptr);
    sqlite3_bind_text(stmt, 1, stripe_id.c_str(), stripe_id.length(), nullptr);
    sqlite3_bind_text(stmt, 2, segment_id.c_str(), segment_id.length(), nullptr);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

void data_manager::db_insert_erasure_share(const erasure_share &es)
{
    const std::string &erasure_share_id = to_string(es.id);
    const std::string &stripe_id = to_string(es.stripe_id);
    const std::string &piece_id = to_string(es.piece_id);
    const char *sql_insert = "insert into \"erasure_share\"(\"id\", \"x_index\", \"y_index\", \"stripe_id\", \"piece_id\")\n"
                             "values (?, ?, ?, ?, ?);";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(sql, sql_insert, -1, &stmt, nullptr);
    sqlite3_bind_text(stmt, 1, piece_id.c_str(), erasure_share_id.length(), nullptr);
    sqlite3_bind_int(stmt, 2, es.x_index);
    sqlite3_bind_int(stmt, 3, es.y_index);
    sqlite3_bind_text(stmt, 4, stripe_id.c_str(), stripe_id.length(), nullptr);
    sqlite3_bind_text(stmt, 5, piece_id.c_str(), piece_id.length(), nullptr);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

void data_manager::db_insert_piece(const piece &p)
{
    const std::string &piece_id = to_string(p.id);
    const std::string &storage_node_id = to_string(p.storage_node_id);
    const char *sql_insert = "insert into \"piece\"(\"id\", \"storage_node_id\")\n"
                             "values (?, ?);";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(sql, sql_insert, -1, &stmt, nullptr);
    sqlite3_bind_text(stmt, 1, piece_id.c_str(), piece_id.length(), nullptr);
    sqlite3_bind_text(stmt, 2, storage_node_id.c_str(), storage_node_id.length(), nullptr);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

void data_manager::db_stmt_select_file(sqlite3_stmt *stmt, file *file)
{
    boost::uuids::string_generator sg;
    file->id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 1)));
    file->name = std::string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 2)));
    file->cfg.file_size = sqlite3_column_int(stmt, 3);
    file->cfg.segment_size = sqlite3_column_int(stmt, 4);
    file->cfg.stripe_size = sqlite3_column_int(stmt, 5);
    file->cfg.erasure_share_size = sqlite3_column_int(stmt, 6);
    file->cfg.k = sqlite3_column_int(stmt, 7);
    file->cfg.m = sqlite3_column_int(stmt, 8);
    file->cfg.n = sqlite3_column_int(stmt, 9);
}

file data_manager::db_select_file_by_id(const std::string &id)
{
    file res;
    const char *sql_select = "select *\n"
                             "from \"file\"\n"
                             "where \"id\" = ?;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr) != SQLITE_OK) {
        return res;
    }
    sqlite3_bind_text(stmt, 1, id.c_str(), id.length(), nullptr);
    sqlite3_step(stmt);
    db_stmt_select_file(stmt, &res);
    sqlite3_finalize(stmt);
    return res;
}

file data_manager::db_select_file_by_name(const std::string &filename)
{
    file res;
    const char *sql_select = "select *\n"
                             "from \"file\"\n"
                             "where \"file_name\" = ?;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr) != SQLITE_OK) {
        return res;
    }
    sqlite3_bind_text(stmt, 1, filename.c_str(), filename.length(), nullptr);
    sqlite3_step(stmt);
    db_stmt_select_file(stmt, &res);
    sqlite3_finalize(stmt);
    return res;
}

segment data_manager::db_select_segment(const std::string &id)
{
    boost::uuids::string_generator sg;
    segment res;
    const char *sql_select = "select *\n"
                             "from \"segment\"\n"
                             "where \"id\" = ?;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr) != SQLITE_OK) {
        return res;
    }
    sqlite3_bind_text(stmt, 1, id.c_str(), id.length(), nullptr);
    sqlite3_step(stmt);
    res.id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 1)));
    res.index = sqlite3_column_int(stmt, 2);
    res.file_id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 3)));
    sqlite3_finalize(stmt);
    return res;
}

stripe data_manager::db_select_stripe(const std::string &id)
{
    boost::uuids::string_generator sg;
    stripe res;
    const char *sql_select = "select *\n"
                             "from \"stripe\"\n"
                             "where \"id\" = ?;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr) != SQLITE_OK) {
        return res;
    }
    sqlite3_bind_text(stmt, 1, id.c_str(), id.length(), nullptr);
    sqlite3_step(stmt);
    res.id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 1)));
    res.index = sqlite3_column_int(stmt, 2);
    res.segment_id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 3)));
    sqlite3_finalize(stmt);
    return res;
}

erasure_share data_manager::db_select_erasure_share(const std::string &id)
{
    boost::uuids::string_generator sg;
    erasure_share res;
    const char *sql_select = "select *\n"
                             "from \"erasure_share\"\n"
                             "where \"id\" = ?;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr) != SQLITE_OK) {
        return res;
    }
    sqlite3_bind_text(stmt, 1, id.c_str(), id.length(), nullptr);
    sqlite3_step(stmt);
    res.id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 1)));
    res.x_index = sqlite3_column_int(stmt, 2);
    res.y_index = sqlite3_column_int(stmt, 3);
    res.stripe_id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 4)));
    res.piece_id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 5)));
    sqlite3_finalize(stmt);
    return res;
}

piece data_manager::db_select_piece(const std::string &id)
{
    boost::uuids::string_generator sg;
    piece res;
    const char *sql_select = "select *\n"
                             "from \"piece\"\n"
                             "where \"id\" = ?;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr) != SQLITE_OK) {
        return res;
    }
    sqlite3_bind_text(stmt, 1, id.c_str(), id.length(), nullptr);
    sqlite3_step(stmt);
    res.id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 1)));
    res.storage_node_id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 2)));
    sqlite3_finalize(stmt);
    return res;
}

void data_manager::db_remove_file_by_id(const std::string &id)
{
    const char *sql_remove = "delete\n"
                             "from \"file\"\n"
                             "where \"id\" = ?;";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(sql, sql_remove, -1, &stmt, nullptr);
    sqlite3_bind_text(stmt, 1, id.c_str(), id.length(), nullptr);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

void data_manager::db_remove_file_by_name(const std::string &name)
{
    const char *sql_remove = "delete\n"
                             "from \"file\"\n"
                             "where \"name\" = ?;";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(sql, sql_remove, -1, &stmt, nullptr);
    sqlite3_bind_text(stmt, 1, name.c_str(), name.length(), nullptr);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

void data_manager::db_remove_segment(const std::string &id)
{
    const char *sql_remove = "delete\n"
                             "from \"segment\"\n"
                             "where \"id\" = ?;";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(sql, sql_remove, -1, &stmt, nullptr);
    sqlite3_bind_text(stmt, 1, id.c_str(), id.length(), nullptr);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

void data_manager::db_remove_stripe(const std::string &id)
{
    const char *sql_remove = "delete\n"
                             "from \"stripe\"\n"
                             "where \"id\" = ?;";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(sql, sql_remove, -1, &stmt, nullptr);
    sqlite3_bind_text(stmt, 1, id.c_str(), id.length(), nullptr);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

void data_manager::db_remove_erasure_share(const std::string &id)
{
    const char *sql_remove = "delete\n"
                             "from \"erasure_share\"\n"
                             "where \"id\" = ?;";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(sql, sql_remove, -1, &stmt, nullptr);
    sqlite3_bind_text(stmt, 1, id.c_str(), id.length(), nullptr);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

void data_manager::db_remove_piece(const std::string &id)
{
    const char *sql_remove = "delete\n"
                             "from \"piece\"\n"
                             "where \"id\" = ?;";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(sql, sql_remove, -1, &stmt, nullptr);
    sqlite3_bind_text(stmt, 1, id.c_str(), id.length(), nullptr);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

void data_manager::upload_piece(const piece &p, const storage_node &node)
{
    const std::string &piece_path = get_piece_path(to_string(node.id), to_string(p.id));
    // 创建文件
    int fd = open(piece_path.c_str(), O_CREAT | O_EXCL | O_RDWR);
    if (fd == -1) {
        perror("upload piece: Failed to open file");
        return;
    }
    // 写内容
    for (int i = 0; i < p.data.size(); i += 2048) {
        int n = std::min(2048, (int) p.data.size() - i);
        if (write(fd, p.data.data() + i, n) <= 0) {
            perror("upload piece: Failed to write file");
            return;
        }
    }
    // 关闭文件
    close(fd);
}

piece data_manager::download_piece(const std::string &piece_id)
{
    piece piece = db_select_piece(piece_id);
    const std::string &piece_path = get_piece_path(to_string(piece.storage_node_id), to_string(piece.id));
    // 创建文件
    int fd = open(piece_path.c_str(), O_RDONLY);
    if (fd == -1) {
        perror("download piece: Failed to open file");
        return piece;
    }
    // 读内容
    int i = 0;
    char buf[2048];
    int n;
    while ((n = read(fd, piece.data.data() + i, 2048)) > 0) {
        piece.data.insert(piece.data.end(), buf, buf + n);
        i += n;
    }
    // 关闭文件
    close(fd);
    return piece;
}

void data_manager::remove_piece(const std::string &piece_id)
{
    const std::string &path = get_piece_path(piece_id);
    if (remove(path.c_str()) == -1) {
        perror("remove piece: Failed to remove piece file");
        return;
    }
    db_remove_piece(piece_id);
}

bool data_manager::audit_piece(const std::string &piece_id)
{
    piece piece = db_select_piece(piece_id);
    const std::string &piece_path = get_piece_path(to_string(piece.storage_node_id), to_string(piece.id));
    int fd = open(piece_path.c_str(), O_RDONLY);
    if (fd <= 0) {
        return false;
    }
    close(fd);
    return true;
}

/**
 * 以指定 (k, m, n) 等配置上传指定文件
 * <ol>
 * <li> 读取 file 内容，切割成 segments
 * <li> 遍历 segments，切割成 stripes
 * <li> 遍历 stripes，纠删编码为 erasure shares
 * <li> 组合 erasure shares，拼接成 pieces
 * <li> pieces 分发到各个 storage nodes
 * </ol>
 * @param filename 文件名
 * @param cfg 配置
 */
void data_manager::upload_file(const std::string &filename, const config &cfg)
{
    data_processor dp(cfg);

    // 随机生成 ID，记录数据对应关系到数据库
    boost::uuids::random_generator uuid_v4;
    file file(filename, cfg);
    file.id = uuid_v4();
    db_insert_file(file);

    // 读文件，切割成 segment 并遍历
    std::vector<segment> segments = dp.split_file(file);
    for (int segment_index = 0; segment_index < segments.size(); segment_index++) {
        segment &segment = segments[segment_index];
        // segment id
        segment.id = uuid_v4();
        segment.index = segment_index;
        segment.file_id = file.id;
        db_insert_segment(segment);
        // 切割成 stripes 并遍历
        std::vector<stripe> stripes = dp.split_segment(segment);
        std::vector<std::vector<erasure_share>> s;
        s.reserve(stripes.size());
        for (int stripe_index = 0; stripe_index < stripes.size(); stripe_index++) {
            stripe &stripe = stripes[stripe_index];
            // stripe id
            stripe.id = uuid_v4();
            stripe.index = stripe_index;
            stripe.segment_id = segment.id;
            db_insert_stripe(stripe);
            // 编码成 erasure shares，该数组为纵向
            std::vector<erasure_share> shares = dp.erasure_encode(stripe);
            for (auto &share: shares) {
                // erasure share id
                share.id = uuid_v4();
                share.stripe_id = stripe.id;
            }
            s.emplace_back(shares);
        }

        // erasure shares 横向合并成 pieces
        std::vector<piece> pieces = dp.merge_to_pieces(s);
        for (auto &piece: pieces) {
            // piece id
            piece.id = uuid_v4();
            for (auto &share: piece.erasure_shares) {
                share.piece_id = piece.id;
                db_insert_erasure_share(share);
            }
        }

        // 上传 pieces 到各个存储节点
        auto piece = pieces.begin();
        auto storage_node = storage_nodes.begin();
        while (piece != pieces.end()) {
            piece->storage_node_id = storage_node->id;
            db_insert_piece(*piece);
            upload_piece(*piece, *storage_node);
            piece++;
            storage_node++;
            // 遍历到最后一个存储节点后，从第一个重新开始遍历
            if (storage_node == storage_nodes.end()) {
                storage_node = storage_nodes.begin();
            }
        }
    }
}

/**
 * 下载指定文件
 * <ol>
 * <li> 读取数据库，得到 file 元数据
 * <li> 查询 file 对应的 segments
 * <li> 以 segment 为单位，查询对应的 pieces 元数据
 * <li> 从相应的 storage nodes 下载得到 pieces
 * <li> 查询 pieces 对应的所有 stripes, erasure shares 元数据
 * <li> 解析 pieces 为 erasure shares
 * <li> erasure shares 纠删解码成 stripes
 * <li> stripes 拼接成 segments
 * <li> segments 拼接成 file
 * </ol>
 * @param filename 文件名
 * @return 文件
 */
file data_manager::download_file(const std::string &filename)
{
    // 从数据库中查出对应的 file 数据
    file file = db_select_file_by_name(filename);
    data_processor dp(file.cfg);

    // 从数据库中查出对应的 piece 数据
    // 建立 segment id 到 pieces 的映射
    boost::uuids::string_generator sg;
    std::unordered_map<std::string, std::vector<piece>> segment_id_to_pieces;
    {
        const char *sql_select = "select \"p\".\"id\",\n"
                                 "       \"sn\".\"id\",\n"
                                 "       \"s\".\"id\"\n"
                                 "from \"file\" \"f\"\n"
                                 "         left join \"stripe\" \"s2\" on \"s\".\"id\" = \"s2\".\"segment_id\"\n"
                                 "         left join \"segment\" \"s\" on \"f\".\"id\" = \"s\".\"file_id\"\n"
                                 "         left join \"erasure_share\" \"es\" on \"s2\".\"id\" = \"es\".\"stripe_id\"\n"
                                 "         left join \"piece\" \"p\" on \"p\".\"id\" = \"es\".\"piece_id\"\n"
                                 "         left join \"storage_node\" \"sn\" on \"sn\".\"id\" = \"p\".\"storage_node_id\"\n"
                                 "where \"f\".\"file_name\" = ?;";
        sqlite3_stmt *stmt;
        if (sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr) != SQLITE_OK) {
            return file;
        }
        sqlite3_bind_text(stmt, 1, filename.c_str(), filename.length(), nullptr);
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            piece p;
            p.id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 1)));
            p.storage_node_id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 2)));
            const std::string &segment_id = std::string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 3)));
            segment_id_to_pieces[segment_id].emplace_back(p);
        }
        sqlite3_finalize(stmt);
    }

    // 遍历映射表，以 segment 为单位处理 piece
    std::vector<segment> segments;
    for (auto &pair: segment_id_to_pieces) {
        const std::string &segment_id = pair.first;
        std::vector<piece> &pieces = pair.second;
        // 从相应的 storage node 下载 piece data
        for (auto &piece: pieces) {
            piece.data = download_piece(to_string(piece.id)).data;
        }

        // 遍历 pieces
        std::vector<std::vector<erasure_share>> s;
        for (const auto &piece: pieces) {
            const std::string &piece_id = to_string(piece.id);
            // piece 拆分成 erasure share（横向）
            std::vector<erasure_share> shares = dp.split_piece(piece);
            // 从数据库查出 erasure share 元数据
            const char *sql_select = "select \"id\",\n"
                                     "       \"x_index\",\n"
                                     "       \"y_index\",\n"
                                     "       \"stripe_id\"\n"
                                     "from \"erasure_share\"\n"
                                     "where \"piece_id\" = ?\n"
                                     "order by \"x_index\", \"y_index\";";
            sqlite3_stmt *stmt;
            if (sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr) != SQLITE_OK) {
                return file;
            }
            sqlite3_bind_text(stmt, 1, piece_id.c_str(), piece_id.length(), nullptr);
            for (int i = 0; sqlite3_step(stmt) == SQLITE_ROW; i++) {
                erasure_share &share = shares[i];
                share.id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 1)));
                share.x_index = sqlite3_column_int(stmt, 2);
                share.y_index = sqlite3_column_int(stmt, 3);
                share.stripe_id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 4)));
            }
            sqlite3_finalize(stmt);
            // 横向排序
            std::sort(shares.begin(), shares.end(), [=](const erasure_share &a, const erasure_share &b) {
                return a.x_index < b.x_index;
            });

            s.emplace_back(shares);
        }

        // erasure shares 二维数组纵向排序
        std::sort(s.begin(), s.end(), [=](const std::vector<erasure_share> &a, const std::vector<erasure_share> &b) {
            return a[0].y_index < b[0].y_index;
        });

        // 选择任意一个 piece，关联查询出 stripes 的元数据
        std::vector<stripe> stripes_metadata;
        {
            const piece &piece = pieces[0];
            const std::string &piece_id = to_string(piece.id);
            const char *sql_select = "select distinct \"s\".\"id\",\n"
                                     "                \"s\".\"index\",\n"
                                     "                \"s\".\"segment_id\"\n"
                                     "from \"piece\" \"p\"\n"
                                     "         left join \"erasure_share\" \"es\" on \"p\".\"id\" = \"es\".\"piece_id\"\n"
                                     "         left join \"stripe\" \"s\" on \"s\".\"id\" = \"es\".\"stripe_id\"\n"
                                     "where \"p\".\"id\" = ?\n"
                                     "order by \"s\".\"index\";";
            sqlite3_stmt *stmt;
            if (sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr) != SQLITE_OK) {
                return file;
            }
            sqlite3_bind_text(stmt, 1, piece_id.c_str(), piece_id.length(), nullptr);
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                stripe stripe;
                stripe.id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 1)));
                stripe.index = sqlite3_column_int(stmt, 2);
                stripe.segment_id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 3)));
                stripes_metadata.emplace_back(stripe);
            }
            sqlite3_finalize(stmt);
        }

        // erasure share decode，纵向拼接成 stripe，赋值元数据
        std::vector<stripe> stripes = dp.merge_to_stripes(s);
        for (int i = 0; i < stripes.size(); i++) {
            stripes[i].id = stripes_metadata[i].id;
            stripes[i].index = stripes_metadata[i].index;
            stripes[i].segment_id = stripes_metadata[i].segment_id;
        }

        // stripe 拼接成 segment，查询出 segment 元数据
        segment segment = db_select_segment(to_string(stripes[0].segment_id));
        segment.data = dp.merge_to_segment(stripes).data;
        segments.emplace_back(segment);
    }
    // segments 排序
    std::sort(segments.begin(), segments.end(), [=](const segment &a, const segment &b) {
        return a.index < b.index;
    });

    // segment 拼接成 file
    file.segments = dp.merge_to_file(segments).segments;
    return file;
}

/**
 * 扫描需要修复的 segments
 * @return segment ids, ks, rs
 */
std::tuple<std::vector<std::string>, std::vector<int>, std::vector<int>> data_manager::scan_corrupted_segments()
{
    std::vector<std::string> segments_to_repair;
    std::vector<int> ks;
    std::vector<int> rs;
    // 查询并遍历所有 file
    std::vector<file> files;
    {
        const char *sql_select = "select *\n"
                                 "from \"file\";";
        sqlite3_stmt *stmt;
        if (sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr) != SQLITE_OK) {
            return {};
        }
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            file file;
            db_stmt_select_file(stmt, &file);
            files.emplace_back(file);
        }
        sqlite3_finalize(stmt);
    }
    for (const auto &file: files) {
        // 查询每个 file 对应的 segments
        std::vector<std::string> segment_ids;
        {
            const char *sql_select = "select \"s\".\"id\"\n"
                                     "from \"file\" \"f\"\n"
                                     "         left join \"segment\" \"s\" on \"f\".\"id\" = \"s\".\"file_id\"\n"
                                     "where \"f\".\"id\" = ?;";
            sqlite3_stmt *stmt;
            if (sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr) != SQLITE_OK) {
                continue;
            }
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                const std::string &segment_id = std::string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1)));
                segment_ids.emplace_back(segment_id);
            }
            sqlite3_finalize(stmt);
        }
        // 以 segment 为单位，查询所有对应的 piece
        for (const auto &segment_id: segment_ids) {
            std::vector<std::string> piece_ids;
            const char *sql_select = "select \"p\".\"id\"\n"
                                     "from \"segment\" \"s\"\n"
                                     "         left join \"stripe\" \"s2\" on \"s\".\"id\" = \"s2\".\"segment_id\"\n"
                                     "         left join \"erasure_share\" \"es\" on \"s2\".\"id\" = \"es\".\"stripe_id\"\n"
                                     "         left join \"piece\" \"p\" on \"p\".\"id\" = \"es\".\"piece_id\"\n"
                                     "where \"s\".\"id\" = ?;";
            sqlite3_stmt *stmt;
            if (sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr) != SQLITE_OK) {
                continue;
            }
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                piece_ids.emplace_back(std::string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1))));
            }
            sqlite3_finalize(stmt);
            // 审计 piece
            for (const auto &piece_id: piece_ids) {
                int count = 0;
                if (audit_piece(piece_id)) {
                    count++;
                }
                // 当剩余 piece 数小于 m 时，将 segment id 和缺失的 piece id 加入到结果中
                if (count <= file.cfg.m) {
                    segments_to_repair.emplace_back(segment_id);
                    ks.emplace_back(file.cfg.k);
                    rs.emplace_back(count);
                }
            }
        }
    }
    return std::make_tuple(segments_to_repair, ks, rs);
}

/**
 * 以 segment 为单位修复，步骤：
 * <ol>
 * <li> 查询对应的 file 配置 (k, m, n)
 * <li> 查询对应的 pieces
 * <li> 下载剩余的 pieces
 * <li> pieces 解析为 erasure shares
 * <li> erasure shares 恢复成 stripes
 * <li> stripes 重新纠删编码成 erasure shares
 * <li> erasure shares 合并成 pieces
 * <li> pieces 分发到各个 storage nodes
 * </ol>
 * @param segment_id
 */
void data_manager::repair_segment(const std::string &segment_id)
{
    // 查询对应的文件配置
    const segment &segment = db_select_segment(segment_id);
    const file &file = db_select_file_by_id(to_string(segment.file_id));
    data_processor dp(file.cfg);
    boost::uuids::random_generator uuid_v4;
    boost::uuids::string_generator sg;
    // 查询所有对应的 piece
    std::vector<std::string> piece_ids;
    {
        const char *sql_select = "select \"p\".\"id\"\n"
                                 "from \"segment\" \"s\"\n"
                                 "         left join \"stripe\" \"s2\" on \"s\".\"id\" = \"s2\".\"segment_id\"\n"
                                 "         left join \"erasure_share\" \"es\" on \"s2\".\"id\" = \"es\".\"stripe_id\"\n"
                                 "         left join \"piece\" \"p\" on \"p\".\"id\" = \"es\".\"piece_id\"\n"
                                 "where \"s\".\"id\" = ?;";
        sqlite3_stmt *stmt;
        if (sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr) != SQLITE_OK) {
            return;
        }
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            piece_ids.emplace_back(std::string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1))));
        }
        sqlite3_finalize(stmt);
    }

    // 下载剩余的 pieces
    std::vector<std::vector<erasure_share>> s;
    std::vector<piece> pieces;
    for (const auto &piece_id: piece_ids) {
        const piece &piece = download_piece(piece_id);
        if (!piece.id.is_nil()) {
            pieces.emplace_back(piece);
            // piece 拆分成 erasure share（横向）
            std::vector<erasure_share> shares = dp.split_piece(piece);
            // 从数据库查出 erasure share 元数据
            const char *sql_select = "select \"id\",\n"
                                     "       \"x_index\",\n"
                                     "       \"y_index\",\n"
                                     "       \"stripe_id\"\n"
                                     "from \"erasure_share\"\n"
                                     "where \"piece_id\" = ?\n"
                                     "order by \"x_index\", \"y_index\";";
            sqlite3_stmt *stmt;
            if (sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr) != SQLITE_OK) {
                perror("repair piece: Failed to prepare SQL");
                continue;
            }
            sqlite3_bind_text(stmt, 1, piece_id.c_str(), piece_id.length(), nullptr);
            for (int i = 0; sqlite3_step(stmt) == SQLITE_ROW; i++) {
                erasure_share &share = shares[i];
                share.id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 1)));
                share.x_index = sqlite3_column_int(stmt, 2);
                share.y_index = sqlite3_column_int(stmt, 3);
                share.stripe_id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 4)));
            }
            sqlite3_finalize(stmt);
            // 横向排序
            std::sort(shares.begin(), shares.end(), [=](const erasure_share &a, const erasure_share &b) {
                return a.x_index < b.x_index;
            });

            s.emplace_back(shares);
        }
    }
    // erasure shares 二维数组纵向排序
    std::sort(s.begin(), s.end(), [=](const std::vector<erasure_share> &a, const std::vector<erasure_share> &b) {
        return a[0].y_index < b[0].y_index;
    });

    // 选择任意一个 piece，关联查询出 stripes 的元数据
    std::vector<stripe> stripes_metadata;
    {
        const piece &piece = pieces[0];
        const std::string &piece_id = to_string(piece.id);
        const char *sql_select = "select distinct \"s\".\"id\",\n"
                                 "                \"s\".\"index\",\n"
                                 "                \"s\".\"segment_id\"\n"
                                 "from \"piece\" \"p\"\n"
                                 "         left join \"erasure_share\" \"es\" on \"p\".\"id\" = \"es\".\"piece_id\"\n"
                                 "         left join \"stripe\" \"s\" on \"s\".\"id\" = \"es\".\"stripe_id\"\n"
                                 "where \"p\".\"id\" = ?\n"
                                 "order by \"s\".\"index\";";
        sqlite3_stmt *stmt;
        if (sqlite3_prepare_v2(sql, sql_select, -1, &stmt, nullptr) != SQLITE_OK) {
            perror("repair piece: Failed to prepare SQL for stripes metadata");
            return;
        }
        sqlite3_bind_text(stmt, 1, piece_id.c_str(), piece_id.length(), nullptr);
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            stripe stripe;
            stripe.id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 1)));
            stripe.index = sqlite3_column_int(stmt, 2);
            stripe.segment_id = sg(reinterpret_cast<const char *const>(sqlite3_column_text(stmt, 3)));
            stripes_metadata.emplace_back(stripe);
        }
        sqlite3_finalize(stmt);
    }

    // erasure shares 恢复成 stripes，计算并修复数据
    std::vector<stripe> stripes = dp.repair_stripes_from_erasure_shares(s);
    s.clear();

    // 新 stripes 处理成 pieces
    // 切割成 stripes 并遍历
    s.reserve(stripes.size());
    for (int stripe_index = 0; stripe_index < stripes.size(); stripe_index++) {
        stripe &stripe = stripes[stripe_index];
        // stripe metadata 结合
        stripe.id = stripes_metadata[stripe_index].id;
        stripe.index = stripes_metadata[stripe_index].index;
        stripe.segment_id = stripes_metadata[stripe_index].segment_id;
        // 编码成 erasure shares，该数组为纵向
        std::vector<erasure_share> shares = dp.erasure_encode(stripe);
        for (auto &share: shares) {
            // erasure share id
            share.id = uuid_v4();
            share.stripe_id = stripe.id;
        }
        s.emplace_back(shares);
    }

    // erasure shares 横向合并成 pieces
    std::vector<piece> pieces_new = dp.merge_to_pieces(s);
    for (auto &piece: pieces_new) {
        // piece id
        piece.id = uuid_v4();
        for (auto &share: piece.erasure_shares) {
            share.piece_id = piece.id;
            db_insert_erasure_share(share);
        }
    }

    // 上传 pieces 到各个存储节点
    auto piece = pieces_new.begin();
    auto storage_node = storage_nodes.begin();
    while (piece != pieces_new.end()) {
        piece->storage_node_id = storage_node->id;
        db_insert_piece(*piece);
        upload_piece(*piece, *storage_node);
        piece++;
        storage_node++;
        // 遍历到最后一个存储节点后，从第一个重新开始遍历
        if (storage_node == storage_nodes.end()) {
            storage_node = storage_nodes.begin();
        }
    }

    // 删除数据库中的旧的 erasure share 和 piece
    // 删除 storage nodes 中的 pieces
    for (const auto &piece_id: piece_ids) {
        const char *sql_remove = "delete\n"
                                 "from \"erasure_share\"\n"
                                 "where \"piece_id\" = ?;";
        sqlite3_stmt *stmt;
        if (sqlite3_prepare_v2(sql, sql_remove, -1, &stmt, nullptr) != SQLITE_OK) {
            continue;
        }
        sqlite3_bind_text(stmt, 1, piece_id.c_str(), piece_id.length(), nullptr);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        remove_piece(piece_id);
    }
}

void data_manager::sort_segments(std::vector<std::string> &segment_ids, std::vector<int> &ks, std::vector<int> &rs)
{
    if (segment_ids.size() != ks.size() || segment_ids.size() != rs.size()) {
        return;
    }
    // 记录 segment id 与 k, r 的对应关系
    std::unordered_map<std::string, std::pair<int, int>> map;
    for (int i = 0; i < segment_ids.size(); i++) {
        map.emplace(segment_ids[i], std::make_pair(ks[i], rs[i]));
    }

    // 评分函数
    const auto &weight = [=](const int k, const int r) {
        double churn_per_round = config::failure_rate * config::total_nodes;
        if (churn_per_round < config::min_churn_per_round) {
            churn_per_round = config::min_churn_per_round;
        }
        double p = double(config::total_nodes - r) / config::total_nodes;
        double mean = double(r - k + 1) * p / (1 - p);
        return mean / churn_per_round;
    };

    // 排序规则
    const auto &less = [=](const std::string &a, const std::string &b) {
        const std::pair<int, int> &p1 = map.at(a);
        const std::pair<int, int> &p2 = map.at(b);
        return weight(p1.first, p1.second) < weight(p2.first, p2.second);
    };

    // 排序
    std::sort(segment_ids.begin(), segment_ids.end(), less);

    // 重新赋值 ks, rs
    for (int i = 0; i < segment_ids.size(); i++) {
        const auto &pair = map[segment_ids[i]];
        ks[i] = pair.first;
        rs[i] = pair.second;
    }
}
