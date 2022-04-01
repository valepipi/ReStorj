//
// Created by ousing9 on 2022/3/9.
//

#ifndef STORJ_EMULATOR_CONFIG_H
#define STORJ_EMULATOR_CONFIG_H


namespace storj
{
    struct config
    {
        unsigned long file_size;
        unsigned long segment_size;
        unsigned long stripe_size;
        unsigned long erasure_share_size;
        unsigned long piece_size;
        int k;
        int m;
        int n;

        static double failure_rate;
        static int total_nodes;
        static double min_churn_per_round;

        config();
    };
}

#endif //STORJ_EMULATOR_CONFIG_H
