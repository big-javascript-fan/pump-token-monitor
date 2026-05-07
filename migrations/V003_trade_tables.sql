-- Open positions & sell ledger (aligned with runtime `init_trade_tables`).

create table if not exists trade_positions (
    id bigserial primary key,
    mint text not null,
    token_name text not null default '',
    buy_tx_signature text not null unique,
    buy_at timestamptz not null default now(),
    buy_price_usd double precision not null default 0,
    token_decimals smallint not null default 9,
    tokens_bought_raw text not null,
    tokens_remaining_raw text not null,
    sol_spent_lamports bigint not null,
    buy_cost_usd_est double precision not null default 0
);

create table if not exists sell_history (
    id bigserial primary key,
    position_id bigint references trade_positions(id) on delete set null,
    mint text not null,
    token_name text not null,
    sell_tx_signature text,
    sold_at timestamptz not null default now(),
    buy_price_usd double precision not null default 0,
    sell_price_usd double precision not null default 0,
    token_decimals smallint not null default 9,
    tokens_sold_raw text not null,
    sol_received_lamports bigint,
    profit_usd double precision not null default 0,
    closed_position boolean not null default false
);
