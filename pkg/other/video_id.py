table = b'fZodR9XQDSUm21yCkr6zBqiveYah8bt4xsWpHnJE7jL5VG3guMTKNPAwcF'  # 码表
tr = bytearray(b'\0' * 128)  # 反查码表
# 初始化反查码表
for i, ti in enumerate(table):
    tr[ti] = i
s = b'\x0b\x0a\x03\x08\x04\x06'  # 位置编码表
xor = 177451812  # 固定异或值
add = 8728348608  # 固定加法值
bv = b'BV1  4 1 7  '

def bv2av(x):
    r = 0
    x = x.encode('ascii')
    for i in range(6):
        r += tr[x[s[i]]] * 58 ** i
    return (r - add) ^ xor

def av2bv(x):
    x = (x ^ xor) + add
    r = bytearray(bv)
    for i in range(6):
        r[s[i]] = table[x // 58 ** i % 58]
    return r.decode('ascii')
