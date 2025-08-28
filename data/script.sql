create table if not exists user_xp
(
    user_id varchar(50) not null
        primary key,
    xp      bigint default 0
);

alter table user_xp
    owner to postgres;

create table if not exists weekly_quests
(
    week   integer not null
        primary key,
    quests jsonb
);

alter table weekly_quests
    owner to postgres;

create table if not exists quest_submissions
(
    id           serial
        primary key,
    user_id      varchar(50),
    quest_number integer,
    proof_link   text,
    status       varchar(20) default 'pending'::character varying
);

alter table quest_submissions
    owner to postgres;

create table if not exists admin_points
(
    id             serial
        primary key,
    total_supply   double precision default '10000000000'::bigint,
    balance        double precision default '10000000000'::bigint,
    in_circulation double precision default 0,
    burned         double precision default 0,
    my_points      double precision default 0,
    treasury       double precision default 0
);

alter table admin_points
    owner to postgres;

create table if not exists points_history
(
    id        serial
        primary key,
    user_id   varchar(50),
    points    double precision,
    purpose   text,
    timestamp timestamp with time zone default now()
);

alter table points_history
    owner to postgres;

create table if not exists giveaway_winners
(
    id        serial
        primary key,
    user_id   varchar(50),
    points    double precision,
    purpose   text,
    timestamp timestamp with time zone default now()
);

alter table giveaway_winners
    owner to postgres;

create table if not exists referrals
(
    referred_id varchar(50) not null
        primary key,
    referrer_id varchar(50),
    status      varchar(20) default 'pending'::character varying
);

alter table referrals
    owner to postgres;

create table if not exists mysterybox_uses
(
    id        serial
        primary key,
    user_id   varchar(50),
    timestamp timestamp with time zone default now()
);

alter table mysterybox_uses
    owner to postgres;

create table if not exists users_points
(
    user_id          bigint                                 not null
        primary key,
    all_time_points  double precision         default 0     not null,
    available_points double precision         default 0     not null,
    xp               bigint                   default 0     not null,
    created_at       timestamp with time zone default now() not null,
    updated_at       timestamp with time zone default now() not null
);

alter table users_points
    owner to postgres;

create table if not exists submissions
(
    id          bigserial
        primary key,
    user_id     bigint                                 not null,
    proof_link  text                                   not null,
    engagements text[],
    status      text                                   not null,
    reward_pts  double precision         default 0     not null,
    ts          timestamp with time zone default now() not null
);

alter table submissions
    owner to postgres;

create table if not exists vip_posts
(
    id        bigserial
        primary key,
    user_id   bigint                                        not null,
    post_link text                                          not null,
    post_date date                     default CURRENT_DATE not null,
    ts        timestamp with time zone default now()        not null
);

alter table vip_posts
    owner to postgres;

create table if not exists gm_log
(
    id      bigserial
        primary key,
    user_id bigint                                 not null,
    message text                                   not null,
    ts      timestamp with time zone default now() not null
);

alter table gm_log
    owner to postgres;

create table if not exists giveaway_logs
(
    id      bigserial
        primary key,
    user_id bigint                                 not null,
    points  double precision                       not null,
    purpose text                                   not null,
    ts      timestamp with time zone default now() not null
);

alter table giveaway_logs
    owner to postgres;

create table if not exists all_time_giveaway_logs
(
    id      bigserial
        primary key,
    user_id bigint                                 not null,
    points  double precision                       not null,
    purpose text                                   not null,
    ts      timestamp with time zone default now() not null
);

alter table all_time_giveaway_logs
    owner to postgres;

create table if not exists referral_data
(
    user_id     bigint                                 not null
        primary key,
    referrer_id bigint                                 not null,
    status      text                                   not null,
    joined_at   timestamp with time zone default now() not null,
    updated_at  timestamp with time zone default now() not null
);

alter table referral_data
    owner to postgres;

create table if not exists pending_referrals
(
    user_id     bigint                                 not null
        primary key,
    referrer_id bigint                                 not null,
    joined_at   timestamp with time zone default now() not null
);

alter table pending_referrals
    owner to postgres;

create table if not exists active_tickets
(
    ticket_id  bigserial
        primary key,
    user_id    bigint                                 not null,
    channel_id bigint                                 not null,
    opened_at  timestamp with time zone default now() not null,
    closed_at  timestamp with time zone
);

alter table active_tickets
    owner to postgres;

create table if not exists bot_data
(
    key   text  not null
        primary key,
    value jsonb not null
);

alter table bot_data
    owner to postgres;

create table if not exists processed_reactions
(
    message_id bigint not null
        primary key
);

alter table processed_reactions
    owner to postgres;

create table if not exists referred_users
(
    user_id bigint not null
        primary key
);

alter table referred_users
    owner to postgres;

create table if not exists user_points
(
    user_id          integer,
    all_time_points  integer,
    available_points integer
);

alter table user_points
    owner to postgres;

create table if not exists approved_proofs
(
    normalized_url TEXT PRIMARY KEY,
    created_at timestamptz not null default now()
);

alter table approved_proofs
    owner to postgres;