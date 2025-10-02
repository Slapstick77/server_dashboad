# Tracked Departments – Definitions & Business Rules

**Last updated:** 2025‑10‑02

This document defines how **Tracked Departments** determine **Department** and **Unit** (COM#/comnumber1) completion and scheduling metrics using data from `SCHSchedulingSummary`, `SCHLabor`, and `DepartmentCode`.

> **Unit synonyms:** `Unit` = `COM#` = `comnumber1` (and minor variations in column naming as they appear in your schema).

---

## 1) Tracked Departments (Canonical)

The following **11** departments are used to determine completion and metrics:

- `Fab`
- `Welding`
- `BaseFormPaint` *(intentional DB spelling; do not change)*
- `FanAssyTest`
- `InsulWallFab`
- `DoorFab`
- `Electrical`
- `Pipe`
- `Paint`
- `Crating`
- `Assembly`

**Completion columns (in `SCHSchedulingSummary`):**  
For each tracked department **D**, the completion percentage is stored in the column **`D Completion`**, e.g.:
- `Fab Completion`
- `Welding Completion`
- `BaseFormPaint Completion`
- `FanAssyTest Completion`
- …
- `Assembly Completion`

> We keep “**BaseFormPaint**” exactly as spelled in the DB.

---

## 2) Data Sources & Mapping

- **`SCHSchedulingSummary`**  
  Holds per‑unit department completion % columns (e.g., `Fab Completion`, `Assembly Completion`) and standard hours (e.g., `stdhours`), used to decide which departments count toward **Unit Complete**.

- **`SCHLabor`**  
  Holds **labor charges** by date, keyed by **labor codes** (e.g., `0120`, `0140`, `0200`, etc.).  
  ⚠️ `SCHLabor` **does not** store department names directly.

- **`DepartmentCode`**  
  Maps **labor codes → department names**. Use this table to resolve a labor code into its department name and then map into **canonical tracked departments** (see crosswalk summary below).

### Crosswalk Summary (names and special rules)

- **Assembly**: Combine **ASSY** and **FLOW** charges. Filtering logic applies uniformly; results are reported as **Assembly**.  
- **Crating**: **FINISH** maps to **Crating**.  
- **Electrical**: **ELEC** maps to **Electrical**.  
- **FanAssyTest**: When `FanAssyTest Completion = 100`, lookup labor code **0200 (FAN)** in `SCHLabor` (via `DepartmentCode`) for charges/dates.  
- **TEST**: Not tracked for completion. Only appears on the **Gantt**; **no filtering** is applied on the Gantt.

> **Case sensitivity:** Not required since mapping is by **labor code** via `DepartmentCode`.

---

## 3) Completion Definitions

### Department Complete
A tracked department **D** is **Complete** for a given Unit when:
- `SCHSchedulingSummary.[D Completion] = 100` (exactly 100).

> Do **not** compute department First/Last/Days/Span until `D Completion = 100`.

### Unit Complete
A **Unit** is **Complete** when **all tracked departments with standard hours > 0** have `D Completion = 100`.

- Departments with **0 standard hours** are **ignored** for unit completion and unit metrics.

---

## 4) Status Definitions (Department & Unit)

- **Not Started**: `Completion = 0%`
- **In Progress**: `0% < Completion < 100%`
- **Complete**: `Completion = 100%`

**Metrics availability:**
- If **Not Started** or **In Progress**, **metrics are NULL** (no First/Last/Days/Span).  
- Metrics (First/Last/Days/Span) are computed **only when Complete (100%)**.

> The **Gantt chart** always shows **all charged days** from any department (raw `SCHLabor`), unaffected by completion state or filtering rules.

---

## 5) Department Metrics (computed only when `D Completion = 100`)

Let **D** be a tracked department and **U** be a Unit (COM# / comnumber1).

1) **Department First Day (D, U)**  
   - Use `SCHLabor` joined to `DepartmentCode` to resolve labor **codes → departments**.  
   - Map to **canonical department D** (e.g., ASSY & FLOW → Assembly; FINISH → Crating; ELEC → Electrical; FAN code → FanAssyTest).  
   - Apply **Filtering Logic** (see `logic.md`).  
   - Take the **earliest** kept **charge date** for D & U.

2) **Department Last Day (D, U)**  
   - After filtering, take the **latest** kept **charge date** for D & U.

3) **Department Actual Days Worked (D, U)**  
   - The **count of distinct kept charge dates** between First and Last Day (inclusive).

4) **Department Time Span (D, U)**  
   - Inclusive calendar span between First and Last Day:  
     `TimeSpanDays = (LastDay - FirstDay) + 1`.

**No‑labor fallback (official rule):**  
If `D Completion = 100` but there are **no kept labor rows** after filtering, then:
- **First Day = NULL**
- **Last Day = NULL**
- **Actual Days Worked = NULL**
- **Time Span = NULL**

---

## 6) Unit Metrics (computed only when **Unit Complete**)

Consider only **tracked departments with stdhours > 0** and `D Completion = 100`.

- **Unit First Day (U)** = `MIN(Department First Day across eligible D)`
- **Unit Last Day (U)** = `MAX(Department Last Day across eligible D)`
- **Units Actual Days Worked (U)**  
  = **count of distinct kept dates** (after filtering) where **any** eligible department had charges between Unit First and Last Day (inclusive).
- **Unit Time Span (U)**  
  = Inclusive calendar span between Unit First and Unit Last Day.

If the unit is **Not Started** or **In Progress**, all unit metrics are **NULL**.

---

## 7) Edge Cases & Notes

- **Zero-hour departments:** Tracked depts with `stdhours = 0` are **ignored** for unit completion and unit metrics.
- **FanAssyTest special:** Use labor code **0200 (FAN)** for day/charge determination once `FanAssyTest Completion = 100`.
- **Assembly special:** Combine all charges from **ASSY** and **FLOW**, apply filtering, and report as **Assembly**.
- **TEST:** Not part of completion or metrics. **Gantt** shows all raw TEST days with **no filters**.
- **No-labor but 100%:** Department metrics remain **NULL** (no fallback to planned dates).
- **Strict equality:** Completion must be **exactly 100** to compute metrics.

---

## 8) Implementation Tips

- Always resolve `SCHLabor` **codes** to department names via `DepartmentCode`, then map to **canonical** tracked departments.
- Apply the **Filtering Logic** from `logic.md` **before** computing First/Last/Days/Span.
- Maintain the canonical spelling: **`BaseFormPaint`**.

---

## 9) References

- **Filtering logic & parameters:** See `logic.md`
- **Data flow & mappings:** See **Data Flow** section in `logic.md`