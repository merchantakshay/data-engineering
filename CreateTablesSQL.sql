CREATE TABLE coins (
id int(5),
volume_24h_usd float(20,2),
available_supply float(15,1),
coin_id varchar(100),
last_updated_utc int(10),
market_cap_usd float(20,2),
max_supply float(20,2),
coin_name varchar(100),
percent_change_1h float(4,2),
percent_change_24h float(4,2),
percent_change_7d float(4,2),
price_vs_btc float(10,10),
price_usd float(15,10),
rank int(5),
symbol varchar(10),
total_supply float(20,2)
);

CREATE TABLE historical (
record_date date,
open_usd float(10,10),
high_usd float(10,10),
low_usd float(10,10),
close_usd float(10,10),
volume float(15,2),
market_cap_usd float(15,2),
coin_id varchar(20),
id int(5)
);
