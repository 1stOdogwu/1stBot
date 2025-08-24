create table if not exists user_points_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table user_points_table
    owner to postgres;

create table if not exists submissions_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table submissions_table
    owner to postgres;

create table if not exists logs_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table logs_table
    owner to postgres;

create table if not exists vip_posts_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table vip_posts_table
    owner to postgres;

create table if not exists user_xp_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table user_xp_table
    owner to postgres;

create table if not exists weekly_quests_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table weekly_quests_table
    owner to postgres;

create table if not exists quest_submissions_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table quest_submissions_table
    owner to postgres;

create table if not exists gm_log_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table gm_log_table
    owner to postgres;

create table if not exists admin_points_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table admin_points_table
    owner to postgres;

create table if not exists referral_data_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table referral_data_table
    owner to postgres;

create table if not exists pending_referrals_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table pending_referrals_table
    owner to postgres;

create table if not exists active_tickets_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table active_tickets_table
    owner to postgres;

create table if not exists mysterybox_uses_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table mysterybox_uses_table
    owner to postgres;

create table if not exists approved_proofs_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table approved_proofs_table
    owner to postgres;

create table if not exists points_history_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table points_history_table
    owner to postgres;

create table if not exists giveaway_logs_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table giveaway_logs_table
    owner to postgres;

create table if not exists all_time_giveaway_logs_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table all_time_giveaway_logs_table
    owner to postgres;

create table if not exists referred_users_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table referred_users_table
    owner to postgres;

create table if not exists processed_reactions_table
(
    id   integer not null
        primary key,
    data jsonb
);

alter table processed_reactions_table
    owner to postgres;


