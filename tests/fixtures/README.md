# Synthetic Test Data — CREW_J_TE_EMPLOYEE_PHONE_DW

Fixtures for `tests/fixtures/synthetic_data.py`. All data is deterministic (no random
generators). Rows are designed so each pipeline branch is exercised by at least one row.

---

## Source Schema — CREW_WSTELE_LND (post-SQL, 71 columns)

This is the schema **after** the source SQL push-down in `WSTELE_LND_TDC` applies TRIM /
concatenation expressions. The JDBC query returns these 71 columns directly.

### Core Identity & Metadata (6 columns)

| Column | Type | Sample Values | Notes |
|---|---|---|---|
| `EMP_NBR` | String | `"100000001"`, `"12345"`, `"ABC_INVALID"` | From `TELE_EMP_NBR`. Non-numeric → error path |
| `USER_ID` | String | `"USR001"` | From `TELE_LAST_UPDATED_BY` |
| `PH_COMMENTS` | String | `"John Smith"`, `"  "`, `null` | From `TELE_EMGR_PH_NAME`. Whitespace → null |
| `PH_NBR` | String | `"2125551234"`, `null` | TRIM concat of 3 EMGR phone fields. Null → no EMERGENCY |
| `TELE_LAST_UPDATED_DATE` | String | `"20240101"` | YYYYMMDD |
| `TELE_LAST_UPDATED_TIME` | String | `"120000"` | HHMMSS |

### TEMP Phone (3 columns)

| Column | Type | Sample Values | Notes |
|---|---|---|---|
| `TELE_TEMP_PH_DATE` | String | `"20241231"`, `null` | YYYYMMDD. Combined with TIME to form TEMP_PH_EXP_TS |
| `TELE_TEMP_PH_TIME` | String | `"235900"`, `null` | HHMMSS |
| `TEMP_PH_NBR` | String | `"9175559999"`, `null` | TRIM concat of TEMP phone fields. Non-null → TEMP path |

### BASIC Phone Slots 1–5 (35 columns, 7 per slot)

Repeated for `i` in 1..5:

| Column | Type | Sample Values | Notes |
|---|---|---|---|
| `BASIC_PH_NBR_{i}` | String | `"2125550001"`, `null` | Null → slot filtered out after unpivot |
| `BASIC_PH_ACCESS_{i}` | String | `"  "`, `null` | Whitespace → null after null-clean |
| `BASIC_PH_UNLIST_CD_{i}` | String | `"N"`, `null` | Maps to `UNLISTD_IND` |
| `BASIC_PH_TYPE_{i}` | String | `"W"`, `"C"`, `null` | Maps to `PH_TYPE` |
| `BASIC_PH_HOME_AWAY_CD_{i}` | String | `"H"`, `"A"`, `"B"` | `"B"` → both HOME and AWAY streams |
| `BASIC_PH_COMMENT_{i}` | String | `"Office"`, `null` | Maps to `PH_COMMENTS` on BASIC rows |
| `TEMP_PH_EXP_TS_{i}` | Timestamp | `null` | Passthrough timestamp |

### HOME Priority Schedule — 3 Groups × 7 columns = 21 columns

Repeated for group `g` in 1..3, seq `s` in 1..5:

| Column | Type | Sample Values | Notes |
|---|---|---|---|
| `TELE_HOME_PRI_FROM_{g}` | String | `"0800"`, `"9999"` | HHMM. `"9999"` → invalid time → NULL |
| `TELE_HOME_PRI_TO_{g}` | String | `"1700"`, `null` | HHMM |
| `TELE_HOME_PRI_SEQ_{s}_{g}` | String | `"1"`, `"2"`, `null` | LKP_CALL_PRTY for slot s in group g |

### AWAY Priority Schedule — 3 Groups × 7 columns = 21 columns

Same structure as HOME:

| Column | Type | Sample Values | Notes |
|---|---|---|---|
| `TELE_AWAY_PRI_FROM_{g}` | String | `"0900"`, `"9900"` | `"9900"` → invalid time → NULL |
| `TELE_AWAY_PRI_TO_{g}` | String | `"1800"`, `null` | HHMM |
| `TELE_AWAY_PRI_SEQ_{s}_{g}` | String | `"1"`, `null` | LKP_CALL_PRTY for slot s in group g |

---

## Row Coverage Table (20 rows)

| Row | `EMP_NBR` | Purpose | Routing Path | Key Assertion |
|---|---|---|---|---|
| 1 | `100000001` | Happy path — full EMERGENCY + HOME | EMERGENCY → BASIC(H) → HOME join → FUNNEL | Row counter starts at 1 |
| 2 | `100000002` | Second employee — row counter reset | EMERGENCY → BASIC(H) → HOME join → FUNNEL | CALL_PRTY resets to 1 per EMP_NBR |
| 3 | `100000003` | BASIC(A) + AWAY schedule | BASIC(A) → AWAY join → FUNNEL | AWAY path mirrors HOME |
| 4 | `100000004` | Second AWAY employee | BASIC(A) → AWAY join → FUNNEL | Row counter reset for AWAY |
| 5 | `100000005` | HOME_AWAY_IND='B' | BASIC(B) → **both** HOME and AWAY | Multi-cast: same row in 2 join streams |
| 6 | `100000006` | TEMP phone | TEMP → FUNNEL (CALL_LIST='TEMP') | TEMP_PH_EXP_TS reconstructed from date+time |
| 7 | `100000007` | All 5 BASIC slots | BASIC → unpivot → 5 rows | Pivot produces exactly 5 output rows |
| 8 | `100000008` | 3 HOME schedule groups × 5 slots | HOME_PVT → 15 rows → offset +5/+10 | SEQ2 CALL_PRTY += 5, SEQ3 += 10 |
| 9 | `100000009` | Null PH_COMMENTS | EMERGENCY → null-clean | `PH_COMMENTS` IS NULL in output |
| 10 | `100000010` | Whitespace-only PH_COMMENTS (`"  "`) | EMERGENCY → null-clean | Whitespace → NULL |
| 11 | `100000011` | Whitespace PH_ACCESS (`"  "`) | EMERGENCY → null-clean | `PH_ACCESS` IS NULL in output |
| 12 | `12345` | Short EMP_NBR (5 chars) | EMERGENCY → finalizer | `EMP_NO` = `"000012345"` |
| 13 | `123456789` | Full 9-char EMP_NBR | EMERGENCY → finalizer | `EMP_NO` = `"123456789"` (no padding) |
| 14 | `100000014` | BASIC slots 2–5 are NULL | BASIC → unpivot → only slot 1 survives | NULL slots removed by isvalid filter |
| 15 | `100000015` | Invalid HOME time `"9999"` | HOME schedule → INC_PRTY_TFM | `TELE_HOME_PRI_FROM` = NULL after validation |
| 16 | `100000016` | Invalid AWAY time `"9900"` | AWAY schedule → INC_AWAY_PRTY_TFM | `TELE_AWAY_PRI_FROM` = NULL after validation |
| 17 | `100000017` | Valid EMP_NBR, NULL PH_NBR | BASIC(H) + HOME only (no EMERGENCY) | EMERGENCY filter excludes null PH_NBR |
| 18 | `100000001` | Duplicate EMP_NBR for HOME schedule | HOME join → row_number window | Second partition resets CALL_PRTY |
| 19 | `ABC_INVALID` | Non-numeric EMP_NBR | ERROR path only | `EMP_NBR.cast("int").isNull()` → WSSEN_ERR_SEQ |
| 20 | `EMP-007` | EMP_NBR with hyphen | ERROR path only | Same as row 19 |

---

## Branch Coverage Table

| Branch | Condition (`phone_router.py`) | Rows Exercising |
|---|---|---|
| EMERGENCY out | `EMP_NBR.cast("int").isNotNull() AND PH_NBR.cast("long").isNotNull()` | 1, 2, 9, 10, 11, 12, 13 |
| TEMP out | `EMP_NBR.cast("int").isNotNull() AND TEMP_PH_NBR.cast("long").isNotNull()` | 6 |
| BASIC out | `EMP_NBR.cast("int").isNotNull()` | 1–15, 17 |
| HOME stream | `HOME_AWAY_IND IN ('B','H')` after BASIC unpivot | 1, 2, 5, 8, 15, 18 |
| AWAY stream | `HOME_AWAY_IND IN ('B','A')` after BASIC unpivot | 3, 4, 5, 16 |
| ERROR out | `EMP_NBR.cast("int").isNull()` | 19, 20 |
| No EMERGENCY (null PH_NBR) | BASIC out but not EMERGENCY | 17 |

| Branch | Condition (`basic_phone_pipeline.py`) | Rows Exercising |
|---|---|---|
| Unpivot slot 1 | `BASIC_PH_NBR_1` isvalid("long") | 1–18 |
| Unpivot slots 1–5 | All 5 `BASIC_PH_NBR_{i}` non-null | 7 |
| NULL slot filtered | `BASIC_PH_NBR_{i}` is NULL after pivot | 14 |
| `HOME_AWAY_IND='H'` | Routed to HOME join only | 1, 2, 8, 15, 17, 18 |
| `HOME_AWAY_IND='A'` | Routed to AWAY join only | 3, 4, 16 |
| `HOME_AWAY_IND='B'` | Routed to **both** HOME and AWAY | 5 |

| Branch | Condition (`home_priority_pipeline.py` / `away_priority_pipeline.py`) | Rows Exercising |
|---|---|---|
| SEQ group 1 CALL_PRTY 1–5 | `TELE_HOME_PRI_SEQ_1..5_1` non-null | 1, 2, 8, 15, 18 |
| SEQ group 2 CALL_PRTY 6–10 | `TELE_HOME_PRI_SEQ_1..5_2` non-null (+5 offset) | 8 |
| SEQ group 3 CALL_PRTY 11–15 | `TELE_HOME_PRI_SEQ_1..5_3` non-null (+10 offset) | 8 |
| Valid schedule time | `to_timestamp(HH:MM:00, "HH:mm:ss").isNotNull()` | 1, 2, 3, 4, 5, 8, 18 |
| Invalid HOME time → NULL | `TELE_HOME_PRI_FROM="9999"` fails time parse | 15 |
| Invalid AWAY time → NULL | `TELE_AWAY_PRI_FROM="9900"` fails time parse | 16 |

| Branch | Condition (`phone_finalizer.py`) | Rows Exercising |
|---|---|---|
| `PH_NBR.cast("long").isNotNull()` passes | Valid long-castable phone number | All non-error rows |
| `PH_NBR.cast("long").isNull()` filtered | Null or non-numeric PH_NBR | Row 17 (filtered out) |
| `PH_ACCESS` whitespace → NULL | `(val == "") OR (val < " ")` | 11 |
| `PH_COMMENTS` null passthrough | Already NULL | 9 |
| `PH_COMMENTS` whitespace → NULL | `(val == "") OR (val < " ")` | 10 |
| `EMP_NO` pad 5→9 chars | `lpad(trim(EMP_NBR), 9, "0")` | 12 |
| `EMP_NO` no padding needed | Already 9 chars | 13 |
| `EFF_START_TM` default `"0001"` | No HOME/AWAY schedule assigned | BASIC direct rows |
| `EFF_END_TM` default `"2359"` | No HOME/AWAY schedule assigned | BASIC direct rows |

---

## Expected Output Factories

| Function | Rows Returned | Purpose |
|---|---|---|
| `create_source_data(spark)` | 20 | Full source dataset for integration tests |
| `create_expected_emergency_records(spark)` | 7 | Rows 1, 2, 9, 10, 11, 12, 13 → EMERGENCY stream |
| `create_expected_error_records(spark)` | 2 | Rows 19, 20 → WSSEN_ERR_SEQ |
| `create_expected_temp_records(spark)` | 1 | Row 6 → TEMP stream with TEMP_PH_EXP_TS |
| `create_expected_basic_direct_sample(spark)` | 5 | Row 7 → all 5 unpivoted BASIC slots |
| `create_expected_emp_no_boundary(spark)` | 2 | Rows 12, 13 → EMP_NO zero-padding boundary |

---

## How to Use in Tests

```python
from tests.fixtures.synthetic_data import (
    create_source_data,
    create_expected_emergency_records,
    create_expected_error_records,
)
from chispa import assert_df_equality

def test_emergency_routing(spark):
    source_df = create_source_data(spark)
    emergency_df, *_ = route_by_phone_type(source_df, config)
    expected_df = create_expected_emergency_records(spark)
    assert_df_equality(
        emergency_df.select("EMP_NBR", "CALL_LIST"),
        expected_df.select("EMP_NBR", "CALL_LIST"),
        ignore_row_order=True,
        ignore_nullable=True,
    )
```

All factory functions return a `DataFrame` from the provided `SparkSession`.
No filesystem I/O — data is embedded as Python literals.
