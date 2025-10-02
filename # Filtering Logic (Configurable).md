# Filtering Logic (Configurable)

**Last updated:** 2025‑10‑02

This document defines the **data flow** and **filtering rules** used to compute **Department** and **Unit** metrics. All filters apply **before** selecting First/Last Day and counting Days Worked.

> The **Gantt chart** always shows **all charged days** from any department and **does not apply** these filters or completion rules.

---

## 1) Data Flow & Department Mapping

1) **Raw charges** come from `SCHLabor` and contain **labor codes** (examples):  
   - `0120 → Fab`  
   - `0140 → Welding`  
   - `0200 → FAN (FanAssyTest)`  
   - `0280 → FLOW (Flow/Assembly area)`  
   - `0360 → TEST`  
   *(actual codes defined in `DepartmentCode`)*

2) **Resolve codes** using `DepartmentCode`:  
   `SCHLabor.LaborCode → DepartmentCode → Labor Department Name`

3) **Canonicalize to Tracked Departments** (examples):  
   - **ASSY**, **FLOW** → **Assembly** (apply the same Assembly logic to both; report as Assembly)  
   - **FINISH** → **Crating**  
   - **ELEC** → **Electrical**  
   - **FAN (0200)** → **FanAssyTest**  
   - **TEST** → (used **only** on Gantt; not used for completion/metrics)

4) **Aggregate** by `(Unit, CanonicalDept, WorkDate)` after exclusions:  
   - `TotalHours = sum(hours)`  
   - `DistinctEmployees = count(distinct Employee)` (excluding configured names)

5) **Apply filters** (Min Hours, Employee Override, Gap Filtering) to select **kept dates** for metrics.

---

## 2) Global Filters

### 2.1 Minimum Day Hours Threshold
A department date is **kept** if:
- `TotalHours ≥ MinHours`, **or**
- `DistinctEmployees ≥ EmployeeOverride`

> If below `MinHours` **and** below override count, the date is **dropped** for metrics.

**Default:**  
- `MinHours = 2.0` hours  
- `EmployeeOverride = 2` employees

### 2.2 Exclusion Employees
- Excluded employees **do not count** toward `TotalHours` or `DistinctEmployees` for day selection.  
- They **may** still be used by other non-selection metrics if needed, but **not** for First/Last/Days/Span selection.

**Example list (names are illustrative):**
- `Rawlings, Chris L (1205797)`

> Update this list as needed; see Config Snippet below.

---

## 3) Gap Filtering (Outlier Caps)

**Purpose:** Remove isolated “stray” first/last days that are too far from adjacent activity.

- **First Day:**  
  **Always** apply gap filtering. Keep the earliest candidate date if the gap to the **next** kept charge date is **≤ DeptCap**, or the date has **≥ EmployeeOverride** employees.

- **Last Day:**  
  **Apply** gap filtering **only when the department is 100% complete**.  
  If the department is not complete, metrics are not computed (officially `NULL`), but the **Gantt** will still display all raw days (no filters).

### Department Caps (days)
> Apply a **single cap for Assembly** that covers both ASSY and FLOW, since both are reported as Assembly.

- `Assembly: 12`
- `BaseFormPaint: 7`
- `Crating: 7`
- `DoorFab: 7`
- `Electrical: 7`
- `Fab: 13`
- `FanAssyTest: 22`
- `InsulWallFab: 11`
- `Paint: 7`
- `Pipe: 7`
- `Welding: 8`
- `Test: 22` *(not used for metrics; Gantt ignores filters and shows all days)*

> **Note:** `FlowLine` is **not listed separately**. Activity from FLOW is included under **Assembly** and uses the **Assembly** cap.

---

## 4) Gantt Chart Behavior

- Shows **all charged days** for **all departments** (including TEST).  
- **No** Min Hours, **no** Employee Override, **no** Gap Filtering, **no** Completion dependency.  
- This view is purely for visibility and troubleshooting—**not** the official metrics.

---

## 5) Completion and Metrics Rules (tie-in)

- **Department metrics** (First/Last/Days/Span) are computed **only** when the department’s `Completion = 100`.  
  - If 100% but **no kept labor dates** after filtering → **all metrics NULL**.
- **Unit metrics** are computed **only** when the **Unit is Complete**, i.e., all tracked departments with `stdhours > 0` have `Completion = 100`.

**Statuses:**
- `0%` → **Not Started**
- `0% < % < 100%` → **In Progress**
- `100%` → **Complete**

---

## 6) Config Snippet (example)

> Store this as `config.yaml` or equivalent if you want the app to be configurable at runtime.


min_hours: 2.0
employee_override: 2

exclusions:
  # Employees excluded from day selection logic
  - name: "Rawlings, Chris L"
    id: "1205797"

gap_caps_days:
  Assembly: 12
  BaseFormPaint: 7
  Crating: 7
  DoorFab: 7
  Electrical: 7
  Fab: 13
  FanAssyTest: 22
  InsulWallFab: 11
  Paint: 7
  Pipe: 7
  Welding: 8
  Test: 22   # Not used for metrics; Gantt ignores filters and shows all days

mapping_notes:
  # `SCHLabor` holds labor codes; resolve to names via DepartmentCode, then canonicalize:
  - "ASSY + FLOW -> Assembly (single Assembly cap)"
  - "FINISH -> Crating"
  - "ELEC -> Electrical"
  - "0200 FAN -> FanAssyTest"
