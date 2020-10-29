# prints part of an sql command based on the keys/values of the given dictionary
# Sample output: '(alpha, beta) VALUES ("gamma", "delta")'
def insert_substr(dic):
    items = dic.items()
    cols = [x[0] for x in items]
    vals = [x[1] for x in items]
    return '(' + ', '.join(cols) + ') VALUES (' + ', '.join(vals) + ')'


def update_substr(dic):
    return ', '.join([str(x) + '=' + str(dic[x]) for x in dic])


def asstr(s, qmark='"'):
    val = qmark + str(s) + qmark
    return val
