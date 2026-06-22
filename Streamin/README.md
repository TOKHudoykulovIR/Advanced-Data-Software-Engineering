# Spark Streaming Practice

Enrich restaurant receipts with weather, then use Spark Structured Streaming to
build per-restaurant order-size stats on top of a 2022 baseline.

## Run
```bash
pip install "pyspark<4"
python main.py
```
On Windows you also need `winutils.exe` + `hadoop.dll` on `HADOOP_HOME`.
To rerun clean, delete `data/output/` and `data/checkpoint/` first.

## What it does
- **Phase 1 (2022, batch):** join receipts to the avg temp on the visit date
  (lat/lng rounded to 2 dp), keep temp > 0 °C, compute original cost
  (`total_cost + discount`), bucket by order size, count per restaurant →
  `data/initial_state/`.
- **Phase 2 (2021, stream):** stream 2021 receipts, broadcast the baseline in,
  repeat the same logic, add the counts on top, set `promo_cold_drinks`
  (avg temp > 25 °C) → `data/output/`.

## Result
One row per restaurant in `data/output/`:
```
restaurant, promo_cold_drinks, batch_timestamp, erroneous_data_cnt,
tiny_cnt, small_cnt, medium_cnt, large_cnt, most_popular_order_type
```

## Notes
- Order size uses original cost (no item-count column in the data).
- Weather columns: `lng, lat, avg_tmpr_c, wthr_date, city, country`.

<img width="1919" height="1033" alt="Screenshot 2026-06-22 221929" src="https://github.com/user-attachments/assets/7470ecd2-5a4b-4ec2-8c40-e0e0e60e7b01" />
