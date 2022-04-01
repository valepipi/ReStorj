create table if not exists "file"
(
    "id"                 varchar(64) primary key not null,
    "file_name"          varchar(255)            not null,
    "file_size"          int(11)                 not null,
    "segment_size"       int(11)                 not null,
    "stripe_size"        int(11)                 not null,
    "erasure_share_size" int(11)                 not null,
    "k"                  int(11)                 not null,
    "m"                  int(11)                 not null,
    "n"                  int(11)                 not null
);
create table if not exists "segment"
(
    "id"      varchar(64) primary key not null,
    "index"   int(11)                 not null,
    "file_id" varchar(64)             not null
);
create table if not exists "piece"
(
    "id"              varchar(64) primary key not null,
    "index"           int(11)                 not null,
    "segment_id"      varchar(64)             not null,
    "storage_node_id" int(11)                 not null
);
create table if not exists "storage_node"
(
    "id" varchar(64) primary key not null
);

insert into "file"("id", "file_name", "file_size", "segment_size", "stripe_size", "erasure_share_size", "k", "m", "n")
values (?, ?, ?, ?, ?, ?, ?, ?, ?);
insert into "segment"("id", "index", "file_id")
values (?, ?, ?);
insert into "piece"("id", "index", "segment_id", "storage_node_id")
values (?, ?, ?, ?);
insert into "storage_node"("id")
values (?);

select *
from "file"
where "id" = ?;

select "p"."id",
       "sn"."id",
       "s"."id"
from "file" "f"
         left join "segment" "s" on "f"."id" = "s"."file_id"
         left join "piece" "p" on "s"."id" = "p"."segment_id"
         left join "storage_node" "sn" on "sn"."id" = "p"."storage_node_id"
where "f"."file_name" = ?
order by "s"."index", "p"."index";

select "s"."id"
from "file" "f"
         left join "segment" "s" on "f"."id" = "s"."file_id"
where "f"."id" = ?;

select "p"."id"
from "segment" "s"
         left join "piece" "p" on "s"."id" = "p"."segment_id"
where "s"."id" = ?
order by "p"."index";

select count(1)
from "storage_node";

delete
from "piece"
where "id" = ?;
