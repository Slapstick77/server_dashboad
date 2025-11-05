with open('webapp/blueprints/dashboard.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Keep only lines up to 3659 (before the unreachable code starts)
with open('webapp/blueprints/dashboard.py', 'w', encoding='utf-8') as f:
    f.writelines(lines[:3659])

print(f'Deleted unreachable code. File now has {len(lines[:3659])} lines')
