import random
import string
import os

def random_name(min_len=1, max_len=5):
    length = random.randint(min_len, max_len)
    return ''.join(random.choices(string.ascii_lowercase, k=length))

def generate_insert_statements(table_name, count):
    statements = []
    for i in range(1, count + 1):
        name = random_name()
        age = random.randint(18, 25)
        gpa = round(random.uniform(2.0, 4.0), 2)
        statement = f"insert into {table_name} (id, name, age, gpa) values ({i}, '{name}', {age}, {gpa});"
        statements.append(statement)
    return statements

def write_to_file(filename, statements):
    with open(filename, 'w') as f:
        for stmt in statements:
            f.write(stmt + '\n')

if __name__ == "__main__":
    table_name = "t4"
    num_samples = 30  # 控制生成多少个样例
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    output_file = os.path.join(desktop_path, "output4.txt")

    inserts = generate_insert_statements(table_name, num_samples)
    write_to_file(output_file, inserts)
    print(f"生成了 {num_samples} 条 SQL 插入语句到 {output_file}")
