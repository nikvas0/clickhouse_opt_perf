import subprocess
import json
import random
import time

class ClickHouse:
    # https://github.com/nikvas0/CHDataSkippingTest/blob/master/indices_test.ipynb
    def __init__(self, binary_path_):
        self.bin_path = binary_path_

    def run(self, query, use_json=True, silent=True):
        cmd = [self.bin_path, '-q', query, '-f', 'JSON']
        #print(cmd)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # https://unix.stackexchange.com/a/238185
        out, err = map(lambda x: x.decode('ascii'), proc.communicate())
        if not silent:
            print('CH: ' + out)
            print('CH: ' + err)
        if use_json:
            return json.loads(out)

def getTimes(count, query):
    result = []
    for _ in range(count):
        start = time.time()
        ch.run(query, use_json=True, silent=True)
        end = time.time()
        print(end - start)
        result.append(end - start)
    #print(result)
    return result

def getMinTime(query, count):
    return min(getTimes(count, query))

def getColumn(x):
    return 'c' + str(x)

def genIneq(n):
    c1 = getColumn(random.randint(0, n - 1))
    c2 = getColumn(random.randint(0, n - 1))
    return c1 + ' >= ' + c2

def genConstraint(n):
    return 'CONSTRAINT t' + str(random.randint(0, 1000000)) + ' CHECK ' + genIneq(n)

def genCreateTable(n, m, name):
    res = 'CREATE TABLE ' + name + ' ('
    for i in range(n):
        res += getColumn(i) + ' Int64,'
    res += 'ind UInt64,'
    res += ', '.join([genConstraint(n) for i in range(m)])
    res += ') ENGINE = MergeTree() ORDER BY ind'
    return res

def genRandomQuery(n, d, a, name, settings):
    res = 'EXPLAIN SYNTAX SELECT ind FROM ' + name + ' WHERE '
    for i in range(d):
        if i != 0:
            res += ' AND '
        res += '('
        for j in range(a):
            if j != 0:
                res += ' OR '
            res += genIneq(n)
        res += ')'
        if len(settings) != 0:
            res += ' SETTINGS ' + settings
    return res


tests = [(100, 50), (100, 100), (100, 150), (200, 300), (500, 600), (800, 1000)] # столбцов и ограничений 

D = 20 # дизъюнктов
A = 5 # атомарных формул

Q = 1 # запросов
T = 10 # попыток
SEED = 42


random.seed(SEED)


ch = ClickHouse('/home/nikita/programming/ClickHouse/build/programs/clickhouse-client')
print("TEST Connection")
print(ch.run('SELECT * from system.numbers where number > 10 limit 10'))

min_times_no_opt = []
min_times_graph_opt = []
min_times_z3_opt = []


for n, m in tests:
    ch.run('DROP TABLE IF EXISTS test_opt_perf', use_json=False, silent=False)
    ch.run(genCreateTable(n, m, 'test_opt_perf'), use_json=False, silent=False)

    min_times_no_opt.append([])
    min_times_graph_opt.append([])
    min_times_z3_opt.append([])

    for q in range(Q):
        query = genRandomQuery(n, D, A, 'test_opt_perf', '')

        # no opt
        print('Query ', query)
        min_times_no_opt[-1].append(round(getMinTime(query, T), 3))
        print('RESULT NO     :', ch.run(query))

        # graph opt
        query += 'SETTINGS convert_query_to_cnf = 1, optimize_using_constraints = 1, optimize_substitute_columns = 1, optimize_append_index = 1'
        print('Query ', query)
        min_times_graph_opt[-1].append(round(getMinTime(query, T), 3))
        print('RESULT GRAPH  :', ch.run(query))

        # z3 opt
        query += ', optimize_using_smt = 1'
        print('Query ', query)
        min_times_z3_opt[-1].append(round(getMinTime(query, T), 3))

        print('RESULT SMT    :', ch.run(query))


print("NO OPTIMIZATION:", min_times_no_opt)
print("GRAPH OPTIMIZATION:", min_times_graph_opt)
print("Z3 OPTIMIZATION:", min_times_z3_opt)
