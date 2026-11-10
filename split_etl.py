import ast
from pathlib import Path


path = Path("etl/core/raw_to_core.py")
source = path.read_text(encoding="utf-8-sig")
tree = ast.parse(source)
lines = source.splitlines(keepends=True)

functions = {
    node.name: node
    for node in tree.body
    if isinstance(node, ast.FunctionDef)
}

if "load_cities" in functions and "dimensions" not in functions:
    print("ETL is already split")
    raise SystemExit(0)


def body(nodes):
    return "".join(
        lines[nodes[0].lineno - 1:nodes[-1].end_lineno]
    )


def call(node, name, table):
    return (
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == name
        and len(node.value.args) > 1
        and isinstance(node.value.args[1], ast.Constant)
        and node.value.args[1].value == table
    )


def guarded_loop(node):
    result = lines[node.lineno - 1:node.end_lineno]
    offset = node.body[0].lineno - node.lineno

    result[offset:offset] = [
        "        if table != selected:\n",
        "            continue\n",
    ]
    return "".join(result)


replacements = {}

nodes = functions["dimensions"].body
cut = next(
    i for i, node in enumerate(nodes)
    if call(node, "reject", "body_types")
)
replacements["dimensions"] = (
    "def load_cities(cur):\n"
    + body(nodes[:cut])
    + "\n\n"
    + "def load_body_types(cur):\n"
    + body(nodes[cut:])
)

nodes = functions["children"].body
assert len(nodes) == 3 and isinstance(nodes[1], ast.For)

replacements["children"] = (
    "def children(cur, selected):\n"
    + body(nodes[:1])
    + guarded_loop(nodes[1])
    + '\n    if selected != "price_body_types":\n'
    + "        return\n\n"
    + body(nodes[2:])
)

nodes = functions["auxiliary"].body

migration = next(
    i for i, node in enumerate(nodes)
    if call(node, "reject", "migrations")
)
stats = next(
    i for i, node in enumerate(nodes)
    if call(node, "check_references", "price_group_stats")
)

assert isinstance(nodes[0], ast.For)

replacements["auxiliary"] = (
    "def validate_directions(cur, selected):\n"
    + guarded_loop(nodes[0])
    + "\n\n"
    + "def load_directions(cur, batch_id):\n"
    + body(nodes[1:migration])
    + "\n\n"
    + "def validate_migrations(cur):\n"
    + body(nodes[migration:stats])
    + "\n\n"
    + "def load_group_stats(cur, batch_id, observed_at):\n"
    + body(nodes[stats:])
)

snapshot = ast.get_source_segment(
    source, functions["snapshot"]
)
old = "CREATE TEMP TABLE s_{table} ON COMMIT DROP AS"

assert old in snapshot

replacements["snapshot"] = snapshot.replace(
    old,
    "CREATE TABLE s_{table} AS",
)

results = ast.get_source_segment(
    source, functions["save_results"]
)
replacements["save_results"] = (
    results.replace(
        "def save_results(cur, batch_id):",
        "def save_results(cur, batch_id, selected):",
    )
    .replace(
        "for table in TABLES:",
        "for table in (selected,):",
        1,
    )
)

output = []

for node in tree.body:
    if (
        isinstance(node, ast.If)
        and "__name__" in ast.unparse(node.test)
    ):
        continue

    if isinstance(node, ast.FunctionDef):
        if node.name in ("process_batch", "run"):
            continue

        output.append(
            replacements.get(
                node.name,
                ast.get_source_segment(source, node),
            )
        )
    else:
        output.append(
            ast.get_source_segment(source, node)
        )

output.append(
    '''
def run(raw_batch_id):
    from .raw_to_core_tasks import run as run_tasks
    return run_tasks(raw_batch_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-batch-id", required=True)
    print(
        json.dumps(
            run(parser.parse_args().raw_batch_id),
            ensure_ascii=False,
            indent=2,
        )
    )
'''
)

updated = "\n\n".join(output) + "\n"
compile(updated, str(path), "exec")

backup = path.with_suffix(".before_tasks.py")
if not backup.exists():
    backup.write_text(source, encoding="utf-8")

path.write_text(updated, encoding="utf-8")
print("Updated", path)