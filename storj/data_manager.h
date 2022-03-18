//
// Created by ousing9 on 2022/3/9.
//

#ifndef STORJ_EMULATOR_DATA_MANAGER_H
#define STORJ_EMULATOR_DATA_MANAGER_H


#include <set>
#include <string>

#include "storage_node.h"
#include "piece.h"
#include "segment.h"
#include "file.h"
#include "stripe.h"
#include "erasure_share.h"

namespace storj
{
    class data_manager
    {
    private:
        const int storage_node_num = 100;
        const std::string storage_node_base_path = "./storage_nodes/";

        sqlite3 *sql = nullptr;
        std::set<storage_node> storage_nodes;

        void init();
        void init_db();
        void init_storage_nodes();

        std::string get_storage_node_path(const storage_node &node);
        std::string get_storage_node_path(const std::string &node_id);
        std::string get_piece_path(const std::string &node_id, const std::string &piece_id);
        std::string get_piece_path(const std::string &piece_id);

        void upload_piece(const piece &p, const storage_node &node);
        piece download_piece(const std::string &piece_id);
        void remove_piece(const std::string &piece_id);
        bool audit_piece(const std::string &piece_id);

        void db_insert_file(const file &f);
        void db_insert_segment(const segment &s);
        void db_insert_stripe(const stripe &s);
        void db_insert_erasure_share(const erasure_share &es);
        void db_insert_piece(const piece &p);

        file db_stmt_select_file(sqlite3_stmt *stmt, file *file);
        file db_select_file_by_id(const std::string &id);
        file db_select_file_by_name(const std::string &filename);
        segment db_select_segment(const std::string &id);
        stripe db_select_stripe(const std::string &id);
        erasure_share db_select_erasure_share(const std::string &id);
        piece db_select_piece(const std::string &id);

        void db_remove_file_by_id(const std::string &id);
        void db_remove_file_by_name(const std::string &name);
        void db_remove_segment(const std::string &id);
        void db_remove_stripe(const std::string &id);
        void db_remove_erasure_share(const std::string &id);
        void db_remove_piece(const std::string &id);

    public:
        data_manager();
        virtual ~data_manager();
        void upload_file(const std::string &filename, const config &cfg);
        file download_file(const std::string &filename);
        std::vector<std::string> scan_corrupted_segments();
        void repair_segment(const std::string &segment_id);

    };
}

#endif //STORJ_EMULATOR_DATA_MANAGER_H
