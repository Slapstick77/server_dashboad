# Per-Department Filtering Rules

**Date:** October 2, 2025  
**Status:** ✅ Complete

## Summary

Added support for per-department filtering rules, allowing each department to have its own minimum hours and minimum employee thresholds. This provides fine-grained control over filtering behavior while maintaining the overall unit metrics calculation logic.

---

## Key Features

### 1. Per-Department Configuration

Each of the 12 tracked departments can now have:
- **Min Hours:** Minimum total hours required for a day to be counted
- **Min Employees:** Number of employees required to override ALL filtering rules

### 2. Employee Override Rule (Universal)

**The Rule:** If a day has ≥ `min_employees` **valid (non-excluded)** employees, the day is KEPT regardless of:
- Hours threshold
- Gap filtering

This ensures days with multiple contributors are never filtered out.

### 3. Global vs Department-Specific

- **Global Defaults:** Set baseline values for all departments
- **Department Overrides:** Each department can have custom values
- **Fallback:** If no department-specific value is set, global default is used

---

## Configuration Structure

### New Data Structure (`DEPARTMENT_RULES`)

```python
DEPARTMENT_RULES = {
    'Assembly': {'min_hours': 2.0, 'min_employees': 2},
    'BaseFormPaint': {'min_hours': 2.0, 'min_employees': 2},
    'Crating': {'min_hours': 2.0, 'min_employees': 2},
    'DoorFab': {'min_hours': 2.0, 'min_employees': 2},
    'Electrical': {'min_hours': 2.0, 'min_employees': 2},
    'Fab': {'min_hours': 2.0, 'min_employees': 2},
    'FanAssyTest': {'min_hours': 2.0, 'min_employees': 2},
    'InsulWallFab': {'min_hours': 2.0, 'min_employees': 2},
    'Paint': {'min_hours': 2.0, 'min_employees': 2},
    'Pipe': {'min_hours': 2.0, 'min_employees': 2},
    'Welding': {'min_hours': 2.0, 'min_employees': 2},
    'Test': {'min_hours': 2.0, 'min_employees': 2},
}
```

---

## How It Works

### Filtering Process (Per Department)

For each department, on each day:

1. **Check Exclusion List**
   - If ALL employees are excluded → Day is dropped
   - Continue with non-excluded employees only

2. **Count Valid Employees**
   - Count employees NOT on exclusion list
   - This is the "valid employee count"

3. **Apply Employee Override**
   - If `valid_employees >= dept_min_employees` → **KEEP DAY** (skip all other checks)
   - Otherwise, continue to hours check

4. **Apply Hours Threshold**
   - If `total_hours < dept_min_hours` → **DROP DAY**
   - Otherwise, continue to gap filtering

5. **Apply Gap Filtering** (if applicable)
   - First day: Always check gap to second day
   - Last day: Only check if department is 100% complete
   - If gap > cap AND valid_employees < dept_min_employees → **DROP DAY**

### Unit Metrics Calculation

**Important:** Unit metrics combine the FILTERED results from each department:

```
Unit Days = Union of all filtered days from all 11 departments
Unit First Day = Earliest day across all departments (after filtering)
Unit Last Day = Latest day across all departments (after filtering)
Unit Span = Last Day - First Day + 1
```

This means:
- Each department is filtered independently with its own rules
- Unit metrics use the combined filtered results
- Per-department rules DO NOT affect how unit metrics are calculated, only WHAT data is used

---

## /logic Page Updates

### New Section: "⚙️ Per-Department Filtering Rules"

Displays a grid of all departments with two inputs each:
- **Min Hours:** Decimal input (e.g., 2.0, 1.5, 3.0)
- **Min Employees:** Integer input (e.g., 2, 3, 1)

**Visual Layout:**
```
Assembly          BaseFormPaint     Crating
Min Hours: 2.0h   Min Hours: 2.0h   Min Hours: 2.0h
Min Employees: 2  Min Employees: 2  Min Employees: 2

DoorFab           Electrical        Fab
Min Hours: 2.0h   Min Hours: 2.0h   Min Hours: 2.0h
Min Employees: 2  Min Employees: 2  Min Employees: 2

...etc...
```

### Location

The per-department rules section appears:
- After the "Minimum Day Hours Threshold" section
- Before the "First/Last Day Gap Filtering" section

---

## Code Changes

### Modified Functions

1. **`_resolve_department_days(label, daymap, is_complete)`**
   - Now retrieves department-specific min_hours and min_employees
   - Applies employee override BEFORE hours threshold
   - Uses per-department values if available, falls back to global defaults

2. **`_filter_days_by_gap(label, ordered_days, daymap, is_complete, excl_employees, min_employees)`**
   - Now accepts `min_employees` parameter (department-specific)
   - Counts only valid (non-excluded) employees
   - Uses department's min_employees for override logic

3. **`/logic` Route Handler**
   - Saves per-department rules from form submission
   - Displays current per-department values in grid

### Modified Global Variables

```python
global PROJECT_DAY_RULES, MIN_DAY_HOURS, OUTLIER_CAPS, DEPARTMENT_RULES
```

---

## Examples

### Example 1: Department-Specific Hours Threshold

**Scenario:** Fab department needs 3 hours minimum, others need 2 hours

**Configuration:**
```
Fab: min_hours = 3.0, min_employees = 2
All Others: min_hours = 2.0, min_employees = 2
```

**Result:**
- Fab day with 2.5 hours and 1 employee → **DROPPED** (below 3h, <2 employees)
- Fab day with 2.5 hours and 2 employees → **KEPT** (employee override)
- Paint day with 1.8 hours and 1 employee → **DROPPED** (below 2h, <2 employees)
- Paint day with 1.8 hours and 2 employees → **KEPT** (employee override)

### Example 2: Different Employee Thresholds

**Scenario:** Assembly needs 3 employees to override, others need 2

**Configuration:**
```
Assembly: min_hours = 2.0, min_employees = 3
All Others: min_hours = 2.0, min_employees = 2
```

**Result:**
- Assembly day with 1.5h and 2 employees → **DROPPED** (below 2h, <3 employees)
- Assembly day with 1.5h and 3 employees → **KEPT** (employee override)
- Welding day with 1.5h and 2 employees → **KEPT** (employee override)

### Example 3: Exclusion Employees Don't Count

**Scenario:** Day has 3 total employees (2 excluded, 1 valid)

**Configuration:**
```
All Departments: min_hours = 2.0, min_employees = 2
Exclusion List: [emp1, emp2]
```

**Day Data:**
- Total employees: 3 (emp1, emp2, emp3)
- Valid employees: 1 (emp3)
- Total hours: 1.5h

**Result:**
- Valid employees (1) < min_employees (2) → No override
- Hours (1.5) < min_hours (2.0) → **DROPPED**

---

## Testing Recommendations

1. **Test Default Behavior**
   - Keep all departments at default (2.0h, 2 employees)
   - Verify existing behavior unchanged

2. **Test Single Department Override**
   - Change Fab to 3.0h, 3 employees
   - Verify only Fab filtering changes

3. **Test Employee Override**
   - Create day with 1.5h but 3 employees
   - Verify day is kept due to employee override

4. **Test Exclusion Interaction**
   - Day with 3 total employees (2 excluded)
   - Should count as 1 valid employee

5. **Test Unit Metrics**
   - Change filtering for one department
   - Verify unit metrics still combine correctly

---

## Benefits

✅ **Flexibility:** Each department can have unique filtering rules  
✅ **Precision:** Fine-tune filtering based on department characteristics  
✅ **Control:** Adjust individual departments without affecting others  
✅ **Transparency:** All rules visible and editable in one place  
✅ **Consistency:** Employee override rule works same way everywhere  
✅ **Simplicity:** Unit metrics logic unchanged - just uses filtered results  

---

## Migration Notes

- All departments start with default values (2.0h, 2 employees)
- No behavioral change unless you modify individual department rules
- Existing unit metrics calculations work identically
- Backward compatible with existing data

---

**Configuration:** All settings available at `/logic` page  
**Documentation:** Filter reasons show department-specific thresholds in tooltips
