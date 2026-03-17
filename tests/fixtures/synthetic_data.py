"""Synthetic test data factories for CREW_J_TE_EMPLOYEE_PHONE_DW pipeline.

Generates deterministic test DataFrames for CREW_WSTELE_LND (the single source table),
covering all routing branches, null handling, error paths, join key matching, aggregation
grouping, pivot coverage, and boundary values.

Source schema is the post-SQL schema — 71 columns output by the WSTELE_LND_TDC stage
after the source SQL applies TRIM||TRIM||TRIM concatenations and column renames.

Row inventory (20 rows total):
  Rows  1–2  : Happy path — full EMERGENCY + BASIC(H) + HOME schedule (2 employees)
  Rows  3–4  : BASIC(A) + AWAY schedule path
  Row   5    : BASIC(B) — exercises BOTH HOME and AWAY join streams simultaneously
  Row   6    : TEMP phone path (valid EMP_NBR + valid TEMP_PH_NBR)
  Row   7    : All 5 BASIC slots populated — tests unpivot produces 5 rows
  Row   8    : 3 HOME schedule groups each with 5 slots — tests 15 pivoted rows
  Row   9    : Null PH_COMMENTS (emergency) — null-clean → NULL
  Row  10    : Whitespace-only PH_COMMENTS — null-clean → NULL
  Row  11    : Whitespace PH_ACCESS — null-clean → NULL
  Row  12    : EMP_NBR="12345" (5 chars) — EMP_NO zero-pad to "000012345"
  Row  13    : EMP_NBR="123456789" (9 chars) — no padding needed
  Row  14    : BASIC record with NULL PH_NBR slots — after pivot only NULL slots filtered
  Row  15    : TELE_HOME_PRI_FROM="9999" — invalid time → NULL after INC_PRTY_TFM
  Row  16    : TELE_AWAY_PRI_FROM="9900" — invalid time → NULL after INC_AWAY_PRTY_TFM
  Row  17    : Valid EMP_NBR but NULL PH_NBR — no EMERGENCY, goes to BASIC + HOME only
  Row  18    : Second employee for same HOME schedule — tests row counter reset per EMP_NBR
  Row  19    : Error path — EMP_NBR="ABC_INVALID" (non-numeric) → WSSEN_ERR_SEQ only
  Row  20    : Error path — EMP_NBR="EMP-007" (contains hyphens) → WSSEN_ERR_SEQ only
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE SCHEMA
# Post-SQL schema: 71 columns output by WSTELE_LND_TDC after source SQL.
# Types inferred from DataStage XML SqlType properties and TRIM||concat expressions.
# ─────────────────────────────────────────────────────────────────────────────

def get_source_schema() -> T.StructType:
    """Return the full 71-column schema for CREW_WSTELE_LND post-source-SQL.

    Returns:
        StructType matching the LNK_OUT_TDC output link of WSTELE_LND_TDC.
    """
    fields = [
        # ── Core identity & metadata ──────────────────────────────────────────
        T.StructField("EMP_NBR",               T.StringType(),    True),  # TELE_EMP_NBR
        T.StructField("USER_ID",               T.StringType(),    True),  # TELE_LAST_UPDATED_BY
        T.StructField("PH_COMMENTS",           T.StringType(),    True),  # TELE_EMGR_PH_NAME
        T.StructField("PH_NBR",                T.StringType(),    True),  # TRIM concat EMGR phone
        T.StructField("TELE_LAST_UPDATED_DATE", T.StringType(),   True),  # YYYYMMDD
        T.StructField("TELE_LAST_UPDATED_TIME", T.StringType(),   True),  # HHMMSS
        # ── TEMP phone ────────────────────────────────────────────────────────
        T.StructField("TELE_TEMP_PH_DATE",     T.StringType(),    True),  # YYYYMMDD
        T.StructField("TELE_TEMP_PH_TIME",     T.StringType(),    True),  # HHMMSS
        T.StructField("TEMP_PH_NBR",           T.StringType(),    True),  # TRIM concat TEMP phone
    ]
    # ── BASIC phone slots 1–5 ─────────────────────────────────────────────────
    for i in range(1, 6):
        fields += [
            T.StructField(f"BASIC_PH_NBR_{i}",         T.StringType(),    True),
            T.StructField(f"BASIC_PH_ACCESS_{i}",       T.StringType(),    True),
            T.StructField(f"BASIC_PH_UNLIST_CD_{i}",    T.StringType(),    True),
            T.StructField(f"BASIC_PH_TYPE_{i}",         T.StringType(),    True),
            T.StructField(f"BASIC_PH_HOME_AWAY_CD_{i}", T.StringType(),    True),
            T.StructField(f"BASIC_PH_COMMENT_{i}",      T.StringType(),    True),
            T.StructField(f"TEMP_PH_EXP_TS_{i}",        T.TimestampType(), True),
        ]
    # ── HOME priority schedule — 3 groups × (FROM, TO, SEQ_1–5) ──────────────
    for g in range(1, 4):
        fields += [
            T.StructField(f"TELE_HOME_PRI_FROM_{g}", T.StringType(), True),
            T.StructField(f"TELE_HOME_PRI_TO_{g}",   T.StringType(), True),
        ]
        for s in range(1, 6):
            fields.append(T.StructField(f"TELE_HOME_PRI_SEQ_{s}_{g}", T.StringType(), True))
    # ── AWAY priority schedule — 3 groups × (FROM, TO, SEQ_1–5) ──────────────
    for g in range(1, 4):
        fields += [
            T.StructField(f"TELE_AWAY_PRI_FROM_{g}", T.StringType(), True),
            T.StructField(f"TELE_AWAY_PRI_TO_{g}",   T.StringType(), True),
        ]
        for s in range(1, 6):
            fields.append(T.StructField(f"TELE_AWAY_PRI_SEQ_{s}_{g}", T.StringType(), True))
    return T.StructType(fields)


# ─────────────────────────────────────────────────────────────────────────────
# HELPER: empty row with all-None defaults
# ─────────────────────────────────────────────────────────────────────────────

def _empty_row() -> dict:
    """Return a fully-None row matching the source schema."""
    row: dict = {
        "EMP_NBR": None, "USER_ID": None, "PH_COMMENTS": None, "PH_NBR": None,
        "TELE_LAST_UPDATED_DATE": "20240101", "TELE_LAST_UPDATED_TIME": "120000",
        "TELE_TEMP_PH_DATE": None, "TELE_TEMP_PH_TIME": None, "TEMP_PH_NBR": None,
    }
    for i in range(1, 6):
        row.update({
            f"BASIC_PH_NBR_{i}": None,
            f"BASIC_PH_ACCESS_{i}": None,
            f"BASIC_PH_UNLIST_CD_{i}": "N",
            f"BASIC_PH_TYPE_{i}": None,
            f"BASIC_PH_HOME_AWAY_CD_{i}": None,
            f"BASIC_PH_COMMENT_{i}": "",
            f"TEMP_PH_EXP_TS_{i}": None,
        })
    for g in range(1, 4):
        row.update({
            f"TELE_HOME_PRI_FROM_{g}": None,
            f"TELE_HOME_PRI_TO_{g}": None,
            f"TELE_AWAY_PRI_FROM_{g}": None,
            f"TELE_AWAY_PRI_TO_{g}": None,
        })
        for s in range(1, 6):
            row[f"TELE_HOME_PRI_SEQ_{s}_{g}"] = None
            row[f"TELE_AWAY_PRI_SEQ_{s}_{g}"] = None
    return row


def _row(**overrides) -> dict:
    """Return an empty row with specified field overrides applied."""
    r = _empty_row()
    r.update(overrides)
    return r


# ─────────────────────────────────────────────────────────────────────────────
# MAIN FACTORY: 20 source rows
# ─────────────────────────────────────────────────────────────────────────────

def create_source_data(spark: SparkSession) -> DataFrame:
    """Create 20-row synthetic dataset for CREW_WSTELE_LND.

    Covers all routing paths, null handling, error paths, join key matching,
    aggregation grouping, pivot coverage, and boundary values.

    Returns:
        DataFrame with 20 rows and 71 columns (LNK_OUT_TDC schema).
    """
    rows = [

        # ── HAPPY PATH: EMERGENCY + BASIC(H) + HOME schedule ─────────────────
        # Row 1: Employee 100000001 — emergency phone, 1 basic HOME phone,
        #        HOME schedule group 1 slot 2 → joins to basic slot 1 (CALL_PRTY 1+1=2)
        _row(
            EMP_NBR="100000001",
            USER_ID="JSMITH",
            PH_COMMENTS="Emergency Contact A",
            PH_NBR="2125550001",                # valid int64 → EMERGENCY stream
            BASIC_PH_NBR_1="2125550101",        # valid int64 → BASIC slot 1
            BASIC_PH_ACCESS_1="9",
            BASIC_PH_UNLIST_CD_1="N",
            BASIC_PH_TYPE_1="W",
            BASIC_PH_HOME_AWAY_CD_1="H",        # H → HOME join stream
            TELE_HOME_PRI_FROM_1="0800",         # valid HH:MM time → passes validation
            TELE_HOME_PRI_TO_1="1700",
            TELE_HOME_PRI_SEQ_2_1="2",          # LKP_CALL_PRTY=2, matches BASIC slot 1 (CALL_PRTY=2)
        ),

        # Row 2: Employee 100000002 — different employee for row counter reset test
        #        Same HOME structure to verify CALL_PRTY starts at 1 again (not continuing from emp 100000001)
        _row(
            EMP_NBR="100000002",
            USER_ID="BJONES",
            PH_COMMENTS="Emergency Contact B",
            PH_NBR="2125550002",
            BASIC_PH_NBR_1="2125550201",
            BASIC_PH_HOME_AWAY_CD_1="H",
            TELE_HOME_PRI_FROM_1="0900",
            TELE_HOME_PRI_TO_1="1800",
            TELE_HOME_PRI_SEQ_2_1="2",
        ),

        # ── HAPPY PATH: BASIC(A) + AWAY schedule ─────────────────────────────
        # Row 3: Employee 100000003 — AWAY path (HOME_AWAY_IND='A')
        _row(
            EMP_NBR="100000003",
            USER_ID="RJOHNSON",
            PH_NBR=None,                         # no emergency phone
            BASIC_PH_NBR_1="2125550301",
            BASIC_PH_HOME_AWAY_CD_1="A",         # A → AWAY join stream
            TELE_AWAY_PRI_FROM_1="0900",
            TELE_AWAY_PRI_TO_1="1800",
            TELE_AWAY_PRI_SEQ_2_1="2",           # LKP_CALL_PRTY=2, matches BASIC slot 1
        ),

        # Row 4: Second AWAY employee for row counter reset in AWAY path
        _row(
            EMP_NBR="100000004",
            USER_ID="KWILLIAMS",
            BASIC_PH_NBR_1="2125550401",
            BASIC_PH_HOME_AWAY_CD_1="A",
            TELE_AWAY_PRI_FROM_1="1000",
            TELE_AWAY_PRI_TO_1="1900",
            TELE_AWAY_PRI_SEQ_2_1="2",
        ),

        # ── HAPPY PATH: BASIC(B) — appears in BOTH HOME and AWAY streams ──────
        # Row 5: HOME_AWAY_IND='B' → joins to both HOME_BASIC_JNR and AWAY_BASIC_JNR
        _row(
            EMP_NBR="100000005",
            USER_ID="MDAVIS",
            PH_NBR="2125550005",
            BASIC_PH_NBR_1="2125550501",
            BASIC_PH_HOME_AWAY_CD_1="B",         # B → BOTH HOME and AWAY join streams
            TELE_HOME_PRI_FROM_1="0800",
            TELE_HOME_PRI_TO_1="1700",
            TELE_HOME_PRI_SEQ_2_1="2",
            TELE_AWAY_PRI_FROM_1="0900",
            TELE_AWAY_PRI_TO_1="1800",
            TELE_AWAY_PRI_SEQ_2_1="2",
        ),

        # ── HAPPY PATH: TEMP phone ─────────────────────────────────────────────
        # Row 6: Valid TEMP phone — exercises timestamp_from_ustring + TEMP_TYPE_TFM
        _row(
            EMP_NBR="100000006",
            USER_ID="LMARTIN",
            TEMP_PH_NBR="2125550601",            # valid int64 → TEMP stream
            TELE_TEMP_PH_DATE="20241231",        # YYYYMMDD
            TELE_TEMP_PH_TIME="235900",          # HHMMSS → 2024-12-31 23:59:00
        ),

        # ── PIVOT COVERAGE: all 5 BASIC slots populated ───────────────────────
        # Row 7: 5 basic phones → after BASIC_TYPE_PVT produces 5 rows (CALL_PRTY 1–5)
        _row(
            EMP_NBR="100000007",
            USER_ID="ATAYLOR",
            PH_NBR="2125550007",
            BASIC_PH_NBR_1="2125550711",
            BASIC_PH_NBR_2="2125550712",
            BASIC_PH_NBR_3="2125550713",
            BASIC_PH_NBR_4="2125550714",
            BASIC_PH_NBR_5="2125550715",
            BASIC_PH_HOME_AWAY_CD_1="H",
            BASIC_PH_HOME_AWAY_CD_2="H",
            BASIC_PH_HOME_AWAY_CD_3="H",
            BASIC_PH_HOME_AWAY_CD_4="H",
            BASIC_PH_HOME_AWAY_CD_5="H",
            BASIC_PH_ACCESS_1="9",
            BASIC_PH_TYPE_1="W",
            BASIC_PH_TYPE_2="W",
        ),

        # ── PIVOT COVERAGE: 3 HOME schedule groups (15 pivoted rows) ──────────
        # Row 8: All 3 HOME groups each with all 5 SEQ slots filled
        #        → HOME_SEQ_FNL union produces 15 rows (CALL_PRTY 1–15 after offsets)
        _row(
            EMP_NBR="100000008",
            USER_ID="PWHITE",
            BASIC_PH_NBR_1="2125550801",
            BASIC_PH_HOME_AWAY_CD_1="H",
            # Group 1: slots 1–5 → CALL_PRTY 1–5 (base, no offset)
            TELE_HOME_PRI_FROM_1="0800", TELE_HOME_PRI_TO_1="1700",
            TELE_HOME_PRI_SEQ_1_1="2",   TELE_HOME_PRI_SEQ_2_1="3",
            TELE_HOME_PRI_SEQ_3_1="4",   TELE_HOME_PRI_SEQ_4_1="5",
            TELE_HOME_PRI_SEQ_5_1="6",
            # Group 2: slots 1–5 → CALL_PRTY 6–10 (after +5 offset in HOME_SEQ2_TFM)
            TELE_HOME_PRI_FROM_2="0900", TELE_HOME_PRI_TO_2="1800",
            TELE_HOME_PRI_SEQ_1_2="7",   TELE_HOME_PRI_SEQ_2_2="8",
            TELE_HOME_PRI_SEQ_3_2="9",   TELE_HOME_PRI_SEQ_4_2="10",
            TELE_HOME_PRI_SEQ_5_2="11",
            # Group 3: slots 1–5 → CALL_PRTY 11–15 (after +10 offset in HOME_SEQ3_TFM)
            TELE_HOME_PRI_FROM_3="1000", TELE_HOME_PRI_TO_3="1900",
            TELE_HOME_PRI_SEQ_1_3="12",  TELE_HOME_PRI_SEQ_2_3="13",
            TELE_HOME_PRI_SEQ_3_3="14",  TELE_HOME_PRI_SEQ_4_3="15",
            TELE_HOME_PRI_SEQ_5_3="16",
        ),

        # ── NULL HANDLING: null PH_COMMENTS in EMERGENCY ─────────────────────
        # Row 9: PH_COMMENTS=None → null-clean keeps as NULL
        _row(
            EMP_NBR="100000009",
            PH_NBR="2125550009",
            PH_COMMENTS=None,               # NULL → stays NULL in output
        ),

        # ── NULL HANDLING: whitespace-only PH_COMMENTS ────────────────────────
        # Row 10: "   " → null-clean → NULL
        _row(
            EMP_NBR="100000010",
            PH_NBR="2125550010",
            PH_COMMENTS="   ",              # whitespace-only → NULL after trim_leading_trailing
        ),

        # ── NULL HANDLING: whitespace-only PH_ACCESS ──────────────────────────
        # Row 11: PH_ACCESS_1="   " → null-clean in NULL_VAL_TFM → NULL
        _row(
            EMP_NBR="100000011",
            BASIC_PH_NBR_1="2125551100",
            BASIC_PH_ACCESS_1="   ",        # whitespace → NULL after nulltovalue()+trim
            BASIC_PH_HOME_AWAY_CD_1="H",
            TELE_HOME_PRI_FROM_1="0800", TELE_HOME_PRI_TO_1="1700",
            TELE_HOME_PRI_SEQ_2_1="2",
        ),

        # ── BOUNDARY: EMP_NBR shorter than 9 chars (zero-padding test) ────────
        # Row 12: EMP_NBR="12345" (5 chars) → EMP_NO = "000012345"
        _row(
            EMP_NBR="12345",                # 5 chars → lpad(9,"0") → "000012345"
            PH_NBR="2125551200",
            USER_ID="SHORTEMPID",
        ),

        # ── BOUNDARY: EMP_NBR exactly 9 chars (no padding needed) ─────────────
        # Row 13: EMP_NBR="123456789" → EMP_NO = "123456789" (unchanged)
        _row(
            EMP_NBR="123456789",            # exactly 9 chars → no padding
            PH_NBR="2125551300",
            USER_ID="NINECHARID",
        ),

        # ── NULL HANDLING: NULL BASIC phone slot ──────────────────────────────
        # Row 14: BASIC_PH_NBR_1 valid, BASIC_PH_NBR_2–5 NULL
        #         → after pivot+filter only slot 1 passes isvalid("int64", PH_NBR)
        _row(
            EMP_NBR="100000014",
            BASIC_PH_NBR_1="2125551400",    # valid → survives ALL_BREC_OUT_TFM filter
            BASIC_PH_NBR_2=None,            # NULL → filtered out by isvalid("int64") check
            BASIC_PH_HOME_AWAY_CD_1="H",
        ),

        # ── BOUNDARY: invalid HOME schedule time ──────────────────────────────
        # Row 15: TELE_HOME_PRI_FROM_1="9999" — not a valid HH:MM time
        #         → INC_PRTY_TFM sets TELE_HOME_PRI_FROM=NULL (time validation fails)
        _row(
            EMP_NBR="100000015",
            BASIC_PH_NBR_1="2125551500",
            BASIC_PH_HOME_AWAY_CD_1="H",
            TELE_HOME_PRI_FROM_1="9999",    # invalid time → setnull() in INC_PRTY_TFM
            TELE_HOME_PRI_TO_1="1700",
            TELE_HOME_PRI_SEQ_2_1="2",
        ),

        # ── BOUNDARY: invalid AWAY schedule time ──────────────────────────────
        # Row 16: TELE_AWAY_PRI_FROM_1="9900" — invalid time
        #         → INC_AWAY_PRTY_TFM sets TELE_HOME_PRI_FROM=NULL
        _row(
            EMP_NBR="100000016",
            BASIC_PH_NBR_1="2125551600",
            BASIC_PH_HOME_AWAY_CD_1="A",
            TELE_AWAY_PRI_FROM_1="9900",    # invalid time → NULL
            TELE_AWAY_PRI_TO_1="1800",
            TELE_AWAY_PRI_SEQ_2_1="2",
        ),

        # ── NULL HANDLING: valid EMP_NBR but NULL emergency phone ─────────────
        # Row 17: PH_NBR=NULL → no EMERGENCY output; BASIC still processed
        _row(
            EMP_NBR="100000017",
            PH_NBR=None,                    # NULL → EMERGENCY filter fails; no emergency row
            BASIC_PH_NBR_1="2125551700",
            BASIC_PH_HOME_AWAY_CD_1="H",
        ),

        # ── AGGREGATION: second employee for same HOME schedule key ───────────
        # Row 18: EMP_NBR="100000001" duplicate — tests that row counter in NEW_PRTY_TFM
        #         correctly resets per EMP_NBR partition (Window.partitionBy)
        _row(
            EMP_NBR="100000001",            # same as row 1 — multi-row same emp group
            USER_ID="JSMITH",
            PH_COMMENTS="Work line B",
            PH_NBR=None,
            BASIC_PH_NBR_1="2125550191",    # different phone
            BASIC_PH_HOME_AWAY_CD_1="H",
            TELE_HOME_PRI_FROM_1="0800",
            TELE_HOME_PRI_TO_1="1700",
            TELE_HOME_PRI_SEQ_3_1="3",      # LKP_CALL_PRTY=3, for a different join slot
        ),

        # ── ERROR PATH: non-numeric EMP_NBR ───────────────────────────────────
        # Row 19: "ABC_INVALID" fails isvalid("int32") → LNK_OUT_ERR_SEQ (WSSEN_ERR_SEQ)
        #         Should NOT appear in any phone-type stream (EMERGENCY/TEMP/BASIC/HOME/AWAY)
        _row(
            EMP_NBR="ABC_INVALID",          # not int32 → error route
            PH_NBR="2125551900",
            USER_ID="ERRUSER1",
        ),

        # ── ERROR PATH: EMP_NBR with hyphens ──────────────────────────────────
        # Row 20: "EMP-007" fails cast to int → error route
        _row(
            EMP_NBR="EMP-007",              # non-numeric → error route
            BASIC_PH_NBR_1="2125552000",
            USER_ID="ERRUSER2",
        ),
    ]

    schema = get_source_schema()
    # Convert each dict to a tuple in schema field order
    field_names = [f.name for f in schema.fields]
    tuples = [tuple(r[fn] for fn in field_names) for r in rows]
    return spark.createDataFrame(tuples, schema=schema)


# ─────────────────────────────────────────────────────────────────────────────
# EXPECTED OUTPUT FACTORIES
# ─────────────────────────────────────────────────────────────────────────────

def get_target_schema() -> T.StructType:
    """Return the 14-column schema of TE_EMPLOYEE_PHONE (post-NULL_VAL_TFM).

    Returns:
        StructType for the target table.
    """
    return T.StructType([
        T.StructField("EMP_NBR",       T.StringType(),    True),
        T.StructField("EMP_NO",        T.StringType(),    True),
        T.StructField("PH_LIST",       T.StringType(),    True),
        T.StructField("PH_PRTY",       T.IntegerType(),   True),
        T.StructField("EFF_START_TM",  T.StringType(),    True),
        T.StructField("EFF_END_TM",    T.StringType(),    True),
        T.StructField("PH_NBR",        T.StringType(),    True),
        T.StructField("PH_ACCESS",     T.StringType(),    True),
        T.StructField("PH_COMMENTS",   T.StringType(),    True),
        T.StructField("PH_TYPE",       T.StringType(),    True),
        T.StructField("UNLISTD_IND",   T.StringType(),    True),
        T.StructField("HOME_AWAY_IND", T.StringType(),    True),
        T.StructField("TEMP_PH_EXP_TS", T.TimestampType(), True),
        T.StructField("TD_LD_TS",      T.TimestampType(), True),
    ])


def create_expected_emergency_records(spark: SparkSession) -> DataFrame:
    """Expected rows produced by EMERGENCY routing from create_source_data().

    Emergency rows: source rows with valid EMP_NBR (int32) AND valid PH_NBR (int64).
    From 20-row dataset: rows 1, 2, 5, 9, 12, 13 have valid EMP_NBR + valid PH_NBR.
    (Rows 9/10 have non-null PH_NBR; row 11 has null PH_NBR → excluded.)

    CALL_LIST='EMERGENCY', CALL_PRTY=1, EFF_START_TM='00:00:00', EFF_END_TM='23:59:00'.
    After NULL_VAL_TFM: EFF_START_TM='00:00:00', EFF_END_TM='23:59:00'.

    Returns:
        DataFrame with (EMP_NBR, EMP_NO, PH_LIST, PH_PRTY, EFF_START_TM, EFF_END_TM,
        PH_NBR, PH_ACCESS, PH_COMMENTS) for assertion in integration tests.
    """
    schema = T.StructType([
        T.StructField("EMP_NBR",      T.StringType(), True),
        T.StructField("EMP_NO",       T.StringType(), True),
        T.StructField("PH_LIST",      T.StringType(), True),
        T.StructField("PH_PRTY",      T.IntegerType(), True),
        T.StructField("EFF_START_TM", T.StringType(), True),
        T.StructField("EFF_END_TM",   T.StringType(), True),
        T.StructField("PH_NBR",       T.StringType(), True),
    ])
    # Rows with valid EMP_NBR and valid PH_NBR from create_source_data()
    rows = [
        # (EMP_NBR, EMP_NO, PH_LIST, PH_PRTY, EFF_START_TM, EFF_END_TM, PH_NBR)
        ("100000001", "100000001", "EMERGENCY", 1, "00:00:00", "23:59:00", "2125550001"),
        ("100000002", "100000002", "EMERGENCY", 1, "00:00:00", "23:59:00", "2125550002"),
        ("100000005", "100000005", "EMERGENCY", 1, "00:00:00", "23:59:00", "2125550005"),
        ("100000009", "100000009", "EMERGENCY", 1, "00:00:00", "23:59:00", "2125550009"),
        ("100000010", "100000010", "EMERGENCY", 1, "00:00:00", "23:59:00", "2125550010"),
        ("12345",     "000012345", "EMERGENCY", 1, "00:00:00", "23:59:00", "2125551200"),
        ("123456789", "123456789", "EMERGENCY", 1, "00:00:00", "23:59:00", "2125551300"),
    ]
    return spark.createDataFrame(rows, schema)


def create_expected_error_records(spark: SparkSession) -> DataFrame:
    """Expected rows routed to WSSEN_ERR_SEQ (invalid EMP_NBR).

    Rows 19 and 20 from create_source_data() have non-numeric EMP_NBR.
    All 71 source columns pass through unchanged.

    Returns:
        DataFrame with only EMP_NBR column (for easy assertion of error routing).
    """
    schema = T.StructType([
        T.StructField("EMP_NBR", T.StringType(), True),
    ])
    return spark.createDataFrame(
        [("ABC_INVALID",), ("EMP-007",)],
        schema,
    )


def create_expected_temp_records(spark: SparkSession) -> DataFrame:
    """Expected rows produced by TEMP routing.

    Row 6: valid EMP_NBR + valid TEMP_PH_NBR → TEMP stream.
    CALL_LIST='TEMP', CALL_PRTY=1, EFF_START_TM='00:01:00', EFF_END_TM='23:59:00'.

    Returns:
        DataFrame with (EMP_NBR, PH_LIST, PH_PRTY, EFF_START_TM, EFF_END_TM, PH_NBR).
    """
    schema = T.StructType([
        T.StructField("EMP_NBR",      T.StringType(),  True),
        T.StructField("PH_LIST",      T.StringType(),  True),
        T.StructField("PH_PRTY",      T.IntegerType(), True),
        T.StructField("EFF_START_TM", T.StringType(),  True),
        T.StructField("EFF_END_TM",   T.StringType(),  True),
        T.StructField("PH_NBR",       T.StringType(),  True),
    ])
    return spark.createDataFrame(
        [("100000006", "TEMP", 1, "00:01:00", "23:59:00", "2125550601")],
        schema,
    )


def create_expected_basic_direct_sample(spark: SparkSession) -> DataFrame:
    """Sample of expected BASIC direct records (ALL_BREC_OUT_TFM).

    Row 7 has 5 BASIC slots → after unpivot produces 5 rows (CALL_PRTY 2–6 after +1).
    All 5 PH_NBR values are valid int64 → all 5 survive ALL_BREC_OUT_TFM filter.

    Returns:
        DataFrame with (EMP_NBR, PH_LIST, PH_PRTY, PH_NBR) for row 7's 5 slots.
    """
    schema = T.StructType([
        T.StructField("EMP_NBR",  T.StringType(),  True),
        T.StructField("PH_LIST",  T.StringType(),  True),
        T.StructField("PH_PRTY",  T.IntegerType(), True),
        T.StructField("PH_NBR",   T.StringType(),  True),
    ])
    # Slot 1→CALL_PRTY=1+1=2, slot 2→3, slot 3→4, slot 4→5, slot 5→6 (after BASIC_REC_TFM +1)
    return spark.createDataFrame(
        [
            ("100000007", "BASIC", 2, "2125550711"),
            ("100000007", "BASIC", 3, "2125550712"),
            ("100000007", "BASIC", 4, "2125550713"),
            ("100000007", "BASIC", 5, "2125550714"),
            ("100000007", "BASIC", 6, "2125550715"),
        ],
        schema,
    )


def create_expected_emp_no_boundary(spark: SparkSession) -> DataFrame:
    """Expected EMP_NO values for zero-padding boundary test.

    Row 12: EMP_NBR="12345" → EMP_NO="000012345" (lpad to 9)
    Row 13: EMP_NBR="123456789" → EMP_NO="123456789" (no change)

    Returns:
        DataFrame with (EMP_NBR, EMP_NO).
    """
    schema = T.StructType([
        T.StructField("EMP_NBR", T.StringType(), True),
        T.StructField("EMP_NO",  T.StringType(), True),
    ])
    return spark.createDataFrame(
        [("12345", "000012345"), ("123456789", "123456789")],
        schema,
    )


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE: all factories grouped for test imports
# ─────────────────────────────────────────────────────────────────────────────

__all__ = [
    "get_source_schema",
    "get_target_schema",
    "create_source_data",
    "create_expected_emergency_records",
    "create_expected_error_records",
    "create_expected_temp_records",
    "create_expected_basic_direct_sample",
    "create_expected_emp_no_boundary",
]
